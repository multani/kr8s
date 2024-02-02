#!/usr/bin/env python3

import asyncio
import random
from contextlib import asynccontextmanager
from datetime import datetime, timedelta, timezone

import structlog

from kr8s import NotFoundError
from kr8s.objects import Event, Lease


class LeaseLock:
    def __init__(self, identity: str, namespace: str, name: str):
        self._identity = identity
        self._namespace = namespace
        self._name = name
        self._metadata = {
            "name": self._name,
            "namespace": self._namespace,
        }

        self._lease = None

    def _make_spec(self):
        now = str(datetime.now(timezone.utc).isoformat())
        spec = {
            "holderIdentity": self.identity,
            "leaseDurationSeconds": 15,  # TODO
            "renewTime": now,
            "acquireTime": now,
            "leaseTransitions": 0,
        }
        return spec

    async def create(self) -> Lease:
        spec = self._make_spec()
        lease = await Lease(
            {
                "metadata": self._metadata,
                "spec": spec,
            }
        )
        await lease.create()
        self._lease = lease
        return lease.spec.to_dict()

    async def get(self):
        lease = await Lease.get(self._name, namespace=self._namespace)
        self._lease = lease
        return lease.spec.to_dict()

    async def update(self, spec):
        lease = await Lease(
            {
                "metadata": self._lease.metadata,
                "spec": spec,
            }
        )
        await lease.update()
        self._lease = lease

    async def record_event(self, message: str) -> None:
        event_message = f"{self.identity} {message}"
        print(f"{self._identity}: message={event_message}")
        return
        subject = {
            "kind": Lease.kind,
            "apiVersion": Lease.version,
        }

        now = str(datetime.now(timezone.utc).isoformat())
        event = Event(
            {
                "metadata": {
                    "name": f"{self._name}.{now}",
                    "namespace": self._namespace,
                },
                "eventTime": now,
                # "series": nil,
                "reportingController": "foobar",
                "reportingComponent": "foobar",
                "reportingInstance": "foobar",
                "action": "did",
                "reason": event_message,
                # "regarding": refRegarding,
                # "related": refRelated,
                # "note": message,
                # "type": eventtype,
            }
        )
        # await event.create()

    @property
    def identity(self):
        return self._identity

    def describe(self) -> str:
        return f"{self._namespace}/{self._name}"


class LeaderElector:
    def __init__(self, lock: LeaseLock):
        self.id = lock.identity
        self._lock = lock
        self._observed = None
        self._observed_time = None
        self.log = structlog.get_logger().bind(id=self.id)

    @asynccontextmanager
    async def enter(self):
        try:
            self.log.info("acquiring lock")
            await self._acquire()
            self.log.info("lock acquired")

            task = asyncio.create_task(self._renew())
            yield
            self.log.info("exiting lock")
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass
            self.log.info("releasing lock")
            released = await self._release()
            self.log.info(f"lock released: {released}")
        except Exception as exc:
            self.log.critical(f"unable to enter lock correctly: {exc}")
            raise exc

    async def _acquire(self):
        desc = self._lock.describe()
        while True:
            succeeded = await self._try_acquire_and_renew()
            if not succeeded:
                self.log.error(f"failed to acquire lease {desc}")
                await asyncio.sleep(2)
                continue

            await self._lock.record_event("became leader")
            self.log.info(f"successfully acquired lease {desc}")
            break

    async def _renew(self):
        while True:
            desc = self._lock.describe()

            # TODO: with timeout
            self.log.info("trying to renew lock")
            try:
                await self._try_acquire_and_renew()
            except Exception as exc:
                self.log.error(f"failed to renew lease {desc}: {exc}")
                return

            self.log.info(f"successfully renewed lease {desc}")
            await asyncio.sleep(2)

    async def _release(self):
        if not self.is_leader():
            self.log.info("is not leader, not releasing")
            return True

        spec = self._lock._make_spec()
        spec["holderIdentity"] = ""
        spec["leaseTransitions"] = self._observed["leaseTransitions"]
        spec["leaseDurationSeconds"] = 1
        now = str(datetime.now(timezone.utc).isoformat())
        spec["acquireTime"] = now
        spec["renewTime"] = now

        try:
            await self._lock.update(spec)
        except Exception as exc:
            self.log.info(f"failed to release lock: {exc}")
            return False

        self._set_observed_record(spec)
        await self._lock.record_event("stepped down as leader")
        return True

    async def _try_acquire_and_renew(self) -> bool:
        record = self._lock._make_spec()

        # TODO: 1. Fast path for the leader to update optimistically assuming that
        # the record observed last time is the current version.

        # 2. obtain or create the ElectionRecord
        try:
            old = await self._lock.get()
        except NotFoundError:
            try:
                observed = await self._lock.create()
            except Exception as exc:
                self.log.error(f"error creating lock: {exc}")
                return False

            self.log.info("lock created")
            self._set_observed_record(observed)
            return True

        # 3. Record obtained, check the Identity & Time
        if self._observed != old:
            self._set_observed_record(old)

        now = datetime.now()
        if (
            len(old.get("holderIdentity", "")) > 0
            and self._is_leave_valid(now)
            and not self.is_leader()
        ):
            self.log.warning(f"Lock held by {old['holderIdentity']} and not yet expired")
            return False

        # 4. We're going to try to update. The leaderElectionRecord is set to
        # it's default here. Let's correct it before updating.
        if self.is_leader():
            record["acquireTime"] = old["acquireTime"]
            record["leaseTransitions"] = old["leaseTransitions"]
        else:
            record["leaseTransitions"] = old["leaseTransitions"] + 1

        try:
            await self._lock.update(record)
        except Exception as exc:
            self.log.error(f"failed to update: {exc}")
            return False
        self._set_observed_record(record)

        return True

    def _set_observed_record(self, record):
        self._observed = record
        self._observed_time = datetime.now()

    def _is_leave_valid(self, now) -> bool:
        old = self._observed_time + timedelta(self._observed["leaseDurationSeconds"])
        res = old < now
        return res

    def is_leader(self) -> bool:
        try:
            return self._observed.get("holderIdentity", "") == self._lock.identity
        except:
            return False


async def main():
    ns = "default"
    name = "test123"

    async def leader_task(identity):
        lock = LeaseLock(identity, ns, name)
        leader = LeaderElector(lock)

        while True:
            async with leader.enter():
                work_time = random.randint(1, 5)
                print(f"{identity}: Will work for {work_time}s...")
                await asyncio.sleep(work_time)
                print(f"{identity}: Done!")

            # Simulate some waiting time before picking up more work
            wait_time = random.randint(2, 5)
            print(f"{identity}: will wait for {wait_time}s...")
            await asyncio.sleep(wait_time)
            break

    tasks = []
    workers = 2
    for i in range(workers):
        identity = f"task-{i}"
        task = asyncio.create_task(leader_task(identity))
        tasks.append(task)

    print("controller: Waiting for all tasks to complete...")
    await asyncio.gather(*tasks)
    print("controller: Done!")


if __name__ == "__main__":
    asyncio.run(main())
