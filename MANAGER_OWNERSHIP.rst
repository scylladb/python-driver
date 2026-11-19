.. _manager-ownership:

Cluster and pool manager ownership evaluation
=============================================

This is the staged development evaluation for `issue #1256
<https://github.com/scylladb/python-driver/issues/1256>`_, the design stage of
`#382 <https://github.com/scylladb/python-driver/issues/382>`_. It records the
ownership and migration requirements, evaluates execution arrangements, and
keeps a detailed worked candidate for reviewing the complete contract. It does
not select a runtime engine or introduce a new execution context. It does not
describe the behavior of a released manager runtime.

Read the document with these distinct statuses:

* The source audit, existing behavior and regression coverage describe the
  pinned baseline. Preserve public behavior, the existing query hot path and
  the recovery fixes when evaluating a migration.
* Logical ownership, exact identity, stale-result disposal, observable terminal
  outcomes and migration/deletion coverage are requirements of that evaluation.
  The candidate mechanisms below must be checked against them; a proposal does
  not override a preservation requirement merely by documenting a new failure.
* Dedicated native C/P/L execution, admission records/windows, numerical seeds,
  completion adapters and the reactor-context ``RuntimeError`` are author
  proposals. The detailed protocols and their claimed bounds are a worked
  model to evaluate, not an accepted implementation or a proved compatible
  runtime. Keep questioned claims, their assumptions and counterexamples in
  the evaluation rather than silently deleting a topic or declaring it settled.

The architecture, workload set, performance measurements and acceptable
regressions require recorded maintainer agreement before runtime implementation
starts at #1257. Budgets and the measurement procedure are part of that
agreement. Neither agreement nor benchmark success is claimed here. In
particular, query-capacity/topology changes and reactor-callback errors remain
separate compatibility decisions. The decision and stage-entry gates below
identify what must be resolved before implementation or deletion of an old writer;
choosing this evaluation format supplies none of those approvals.

Baseline and invariants
-----------------------

The original source audit used master
``b4aa97b5cace48f9e97a195a8601ac427304173d``. The complete inventory was
rechecked at landed master ``175493ae9350f1b6fd9dd494f730317b9a3eb1e2``,
which is this document's runtime and pre-migration comparison baseline.
`PR #1255 <https://github.com/scylladb/python-driver/pull/1255>`_ at
``9b000a4db105e785cb304a92b23beb14587358f0`` supplied the separately audited
recovery delta. It landed on master as
``175493ae9350f1b6fd9dd494f730317b9a3eb1e2``. The landed version preserves
FIFO timer/completion delivery and additionally admits initial recovery through
``submit_async`` so the failed-connection caller does not drain callbacks.
The intervening ``ef5a2e93`` routing-key cache and ``e29b6464`` consistency
defaults change query behavior, without moving the inventoried lifecycle
writers. Preserve successful-rebind cache invalidation and the separately
synchronized request path. The Tornado documentation dependency update moves
no runtime ownership. Recheck the full inventory when the baseline or recovery
source changes. Do not compare a migrated tree with an older runtime merely
because that runtime was the original audit base.
The completed `#921 <https://github.com/scylladb/python-driver/issues/921>`_
established host-id identity, immutable endpoints, replacement transactions,
generation fences and regression coverage. It explicitly left the broader
single-owner redesign to #382. Its closure identifies the implementation in
PRs #1006, #1036, #1166 and #1167 and live topology/routing coverage in PRs
#1181, #1183, #1184 and #1217.

``LifecycleCoordinator`` in ``cassandra/_lifecycle.py`` currently serializes
notification delivery, but drains on submitting or executor threads. Its
transactions, Host locks and Session locks still coordinate multiple writers.
PR #1255 moves discounted-DOWN recovery admission, timers and completion
continuations into this FIFO. Keep that prerequisite behavior and its tests; do
not build a second recovery loop when replacing the FIFO runtime.

The migration must preserve these invariants:

* A UUID identifies a host. Exact Host instance and endpoint identify one
  topology incarnation; equality by UUID alone does not prove ownership.
* Same-id endpoint movement creates a replacement Host. Same-endpoint,
  different-id discovery retires the previous node. Endpoint swaps publish
  as one validated batch, not a series of independently visible moves.
* A redundant ADD/UP does not replace an accepted, healthy matching pool.
  A stale failure cannot convict or retire its replacement.
* Request completion returns a stream to the original connection and pool,
  including after retirement. It must not borrow accounting from a newer
  owner or dispatch a statement again because local pool membership changed.
  This preserves the #317 regression; it does not promise exactly-once
  execution across network failures.
* No owner handler waits for network I/O, a worker, another owner, application
  code or a future. No ownership guard spans one of those operations.

Ownership inventory
-------------------

The following is the writer/reader inventory and destination for driver-owned
state. Method names refer to the baseline, not new public APIs. ``C`` denotes
the cluster owner; ``P`` denotes the logical owner of one pool; ``L`` denotes
the Cluster callback lane. A worker may own unpublished candidate data, but
may not publish it into these fields.
The stage numbers are GitHub issues.

.. list-table:: State, existing access and destination
   :header-rows: 1
   :widths: 20 36 25 19

   * - State
     - Current writers and contexts
     - Readers
     - Target / stage
   * - Host admission, retirement, readiness and health generations
     - ``LifecycleCoordinator`` admission/readiness/DOWN/retirement methods;
       Cluster lifecycle handlers; ``Host.set_up/set_down`` and
       ``signal_connection_failure``; pool failure callbacks
     - Policies, listeners, health inspection, Session creation and recovery
     - C / #1258; P reports observations, never edits Host health
   * - Host preparation/addition tokens, pending location, reconnect slots
     - Coordinator preparation/addition/location methods;
       ``Cluster._prepare_host``, retry scheduling and host reconnect handlers
     - Lifecycle continuations, retry timers, readiness aggregation
     - C / #1258; preparation and connection I/O stay in workers
   * - Host UUID, endpoint and topology incarnation
     - Host construction and coordinator replacement publication;
       ``Cluster.add_host/replace_host_endpoint``
     - Metadata, routing, pool ownership and control binding
     - C creates/publishes immutable replacements / #1258
   * - Metadata host and endpoint indexes; location and node attributes
     - ``Metadata._add_or_replace_host`` via ``LifecycleCoordinator.publish_host``;
       ``add_or_return_host`` via ``update_host``; removal and
       ``_update_host_location`` (including token-map replacement);
       ``ControlConnection._apply_node_list_and_token_map``
     - Public host lookup, refresh preparation, policies and routing
     - C / #1258; retain coherent reader publication
   * - Refresh revision, pending forced rebuild, ring and partitioner
     - Coordinator reserve/accept/rebuild methods;
       control topology refresh and ``Metadata.rebuild_token_map``
     - Token-aware query plans, shard routing, refresh completion
     - C accepts/publishes / #1258; workers build candidate ring data
   * - Per-keyspace lazy replica caches in ``TokenMap``
     - ``rebuild_keyspace`` on query readers and schema invalidation paths
     - ``get_replicas`` and load-balancing policies
     - Separate cache synchronization; retain ``_rebuild_lock`` and fence
       cache publication to its TokenMap/schema version / #1258
   * - Policy membership and application listeners
     - Cluster/ProfileManager lifecycle calls, including custom policy code;
       policies own their internal data structures
     - Query plans and distance queries on application/worker threads
     - Ordered callback lane, not C-owned policy internals / #1258
   * - Execution-profile registration, consistency defaults and policy
       initialization
     - ``Cluster.add_execution_profile`` registers, calls ``populate/on_up``,
       then requests Session pool reconciliation before waiting;
       when ``metadata.dbaas`` is true, ``_set_default_dbaas_consistency`` on
       connect and profile addition rewrites every registered non-explicit
       profile's ``consistency_level`` and the Session default, including later
       user assignments to that field. The flag defaults to false and is set
       for the DataStax Cloud product type; other clusters retain their defaults
     - Query profile/consistency lookup, policy distance and Session pool
       reconciliation
     - Initiating API configuration and C-owned pool reconciliation / #1259;
       short native registry synchronization and a producer-demand slot are
       worked-candidate mechanisms, not requirements of every arrangement
   * - Registered sessions and published ``Session._pools``
     - ``Cluster._new_session``; Session constructor, pool workers,
       ``remove_pool``, reconciliation and shutdown
     - Request routing, heartbeat, initial connect and introspection
     - C / #1259; publish pool views for direct request access
   * - Pool generations, intents, creation attempts and completion proxies
     - ``add_or_renew_pool``, ``update_created_pools``, creation/failure
       helpers and executor completion callbacks
     - Creation admission/publication, waiters and stale-work checks
     - C / #1259; workers return candidates/outcomes only
   * - Discounted-DOWN recovery schedule and expected failed pool
     - ``Session._recover_pool``; scheduler and creation completions;
       PR #1255 queues each continuation through lifecycle delivery
     - Reconciliation, removal, supersession and shutdown
     - C / #1259; retain exact failed-pool token and schedule progress
   * - Control socket, generation, pending submission, reconnect intent,
       handler and exhaustion marker
     - ``ControlConnection._set_new_connection``, ``reconnect``,
       ``_raise_reconnect_pending``, ``_clear_reconnect_pending``,
       ``_finish_reconnect``, handler callbacks and shutdown
     - Refresh workers, heartbeat, fallback queries and reconnect timers
     - C / #1261; socket/query work remains asynchronous
   * - Control endpoint/host binding and pending route refreshes
     - Control connect/binding and client-route refresh handlers
     - Refresh admission, fallback execution, route discovery
     - C / #1261; preserve TCP, Unix, SNI and alternate-route bindings
   * - Client-route store and preferred-route selection
     - ``_ClientRoutesHandler.initialize/handle_client_routes_change`` query
       routes and call ``_RouteStore.update/merge``
     - Endpoint factory and ``resolve_host`` during connection preparation
     - C admits versioned route results / #1261; preserve writer locking,
       copy-on-write publication and sticky selection. Current readers use
       lock-free lookups; audit their safety on each supported runtime
   * - Pool shard slots, attempts, replacement markers and connection sets
     - ``HostConnection`` constructor, ``_replace``, missing-shard workers,
       ``_sync_keyspace``, ``return_connection`` and shutdown
     - Borrow/return, shard selection, heartbeat and pool health snapshots
     - P / #1260; publish a usable connection view
   * - Pool keyspace target, USE generation and connection readiness
     - Session keyspace completion; ``_set_keyspace_for_all_conns`` and
       its reply callbacks; candidate ``_presync/_sync_keyspace``
     - Connection admission, request execution and keyspace waiters
     - C owns Session target; P owns member readiness / #1259, #1260
   * - Sharding discovery and advanced shard-aware fallback deadline
     - First connection construction, missing-shard work and
       ``disable_advanced_shard_aware``
     - Pool connection selection and subsequent opens
     - P owns pool discoveries; C publishes Host capability observations
       with incarnation checks / #1260
   * - Trash, excess and pending connections; retirement
     - Replacement/open workers, ``return_connection``,
       ``_close_excess_connections`` and ``shutdown``
     - Draining completion, orphan release and resource cleanup
     - P / #1260; request accounting remains separate
   * - Streams, in-flight counts, orphan IDs and transport state
     - Connection/reactor request and completion paths, heartbeat and close
     - Borrow/return and transport callbacks
     - Existing ``Connection.lock`` and ``HostConnection._stream_available_condition``;
       never owner-mailbox
       round trips per request / #1260 audit
   * - Schema objects, UDT maps and replication strategy changes
     - ``Metadata.refresh/_rebuild_all/_update_* /_drop_*`` via control
       refresh; ``refresh_schema_and_set_result`` after query responses;
       user-type registration/cache paths
     - Prepared statements, decoders, API metadata and replica calculation
     - C admits control/query-driven schema publications / #1261;
       decoder and prepared-cache synchronization stays separate
   * - Tablet routing cache
     - ``ResponseFuture._cache_tablet_from_payload`` on response paths;
       metadata table/keyspace/host invalidation
     - Query-plan and shard/tablet lookup
     - Separate ``Tablets`` lock and replacement-list publication;
       C sends host invalidations through that API / #1258;
       schema/drop invalidations / #1261; payload writes retain their cache API
   * - Scheduler, heartbeat and executor bookkeeping
     - Their own threads, timers, task completion and close methods
     - Owners receive observations/results; close tracks outstanding work
     - Remain service-owned; no direct lifecycle-state writes / #1262

Schema and tablets are not interchangeable with host topology. Split schema
query/parsing from publication, including DDL-response-driven refresh, and
attach a schema operation revision and original control binding. A schema
result must not repopulate a dropped keyspace/table after a later accepted
drop. UDT decoder registrations retain their existing cache contract; this
design does not make every decoder cache cluster-owned.

Tablet payloads remain direct cache updates to avoid a management round trip
per response. ``Tablets.add_tablet`` already replaces the table's list under
its lock, so readers can retain a stable list. Retain that contract and the
drop APIs. The migration audit must test an in-flight payload racing a table
or host invalidation; it must not claim that host ownership alone versions
tablet data. Any change to tablet invalidation generations is a separately
specified cache change with its own tests, not removal of ``Tablets._lock``.

Execution contexts and publication
----------------------------------

C and P name logical state owners, not a settled allocation of threads.
L names the application-callback delivery role. Every arrangement must retain
the full inventory, message identities, publication/lease rules, terminal
outcomes and migration coverage. Owners must not block on I/O, another owner
or application code, and native completion must not wait behind a callback
that is itself waiting for that completion.

.. list-table:: Execution arrangements to evaluate before runtime selection
   :header-rows: 1
   :widths: 24 36 40

   * - Arrangement
     - Concrete shape and benefit
     - Consequences and evidence needed
   * - Dedicated native C; logical P mailboxes
     - One C thread, two P runners shared within one Cluster, and one L
       callback thread, plus one bounded capture-cleanup thread. Existing
       blocking-I/O workers remain separate. C progress is isolated from P
       runner occupancy.
     - Five proposed new threads per Cluster plus existing services;
       cross-thread result/completion bridges and all supported reactor,
       fork and close paths need validation. Counts are sizing hypotheses.
   * - One native management runner for C and P
     - Distinct logical C/P mailboxes share one native runner; L and blocking
       I/O remain separate. With the same capture-cleanup service, this reduces
       handoffs and proposed new threads to three without giving a worker
       publication authority.
     - P work also delays C. Specify bounded handler/scan turns, fair ready
       scheduling and reserved health/close progress. Co-location cannot bypass
       identity checks, leases or the prohibition on synchronous owner waits.
   * - Fixed per-Cluster runners shared by C and P
     - For comparison, two management runners claim at most one drainer per
       mailbox, plus L and the capture-cleanup service. Existing blocking-I/O
       workers remain separate. This permits C/P parallelism with one
       scheduling engine.
     - Define failure-safe ownership handoff, rejected resubmission and fairness
       when P occupies the runners. Per-Cluster sharing has no unrelated-Cluster
       starvation; process-wide sharing is a different option with that cost.
       The proposed runner count is not an agreed budget.
   * - Owners scheduled on supported reactor backends
     - Serial logical mailboxes use each backend's loop callbacks/tasks,
       with blocking I/O and application callbacks outside owner handlers.
       This can reduce dedicated threads and some cross-thread result hops.
     - Asyncio, libev and supported asyncore need complete lifetime, wakeup,
       bounded-turn and native-waiter adapters; an asyncio-only task is not a
       complete option. Management must not starve transport progress. A shared
       loop cannot be stopped merely because one Cluster closes.

No arrangement is selected here. ``AsyncioConnection.initialize_reactor``
creates a driver background event loop; its lifetime must be evaluated from
that source rather than assumed to belong to an application's loop. Libev and
asyncore have their own loop/wakeup implementations. Existing primitives are
building blocks, not proof of complete manager adapters. The existing blocking
I/O executor can be occupied by opens, schema work or query prerequisites;
reusing it for owners requires a separate progress argument, including
``executor_threads=1``. Serializing notification delivery alone does not migrate
all writers into logical ownership.

Worked candidate: dedicated native C and logical P
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The remaining detailed protocols use the first arrangement as a concrete
candidate unless identified as baseline behavior or a preservation requirement.
Its exact mechanisms, bounds and resource counts remain proposals. Other
arrangements must cover the same topics and tests, not inherit this candidate's
thread allocation merely because they use the names C, P and L.

In this candidate, one reactor-independent native cluster-owner thread serves
each Cluster, with logical serial pool mailboxes on a fixed management executor
owned by that Cluster. Two runners are shared only among that Cluster's pools,
not across Clusters and not one thread per pool or shard. Cluster shutdown stops
only its own runners after their obligations drain. Fork children reject
inherited runners/mailboxes and initialize independent resources. These runners
are separate from the existing executor used for blocking opens, queries,
schema agreement and cleanup. A proposed turn processes at most 16 messages
before yielding its mailbox back to the ready queue. At most one runner may
own a given mailbox, including when resubmission fails.

This candidate uses a serial callback lane per Cluster for lifecycle policy
notifications and listeners, with one callback thread on the supported reactors.
Caller-stack profile initialization is a separate baseline path; its overlap
with these notifications is described below. Future completion is a separate
delivery path and cannot queue behind a listener that is waiting for that
future. No owner invokes arbitrary future callbacks
as part of a state transition.

One additional cleanup thread releases handler captures for private-only native
operations, consumed health observations and replaced pending health captures
retired by C/P. Releasing the last reference can invoke a destructor or weakref
callback, so this happens outside runtime and Future locks, without occupying
C, P or the active L callback. Ordinary handler
captures and lifecycle notification batches retain their L disposal path.
Future done-callback captures retire on their bridge or caller drainer; disposal
after delivery failure uses L.
Coalesced work that retains an ordinary reservation also keeps that path; its
private waiter may finish independently while the ordinary charge bounds the
remaining captures. This service does not invoke lifecycle notifications or
registered done callbacks, and is not a pool per Host or shard.

.. code-block:: text

   callers / timers / heartbeat / reactor observations
                         |
                         v
                 cluster mailbox (C) ----> published topology/pool view
                    |          ^                        |
             work request      | accepted result        v
                    v          |                  query/borrow/return
                 I/O workers --+                  (no C mailbox hop)
                    ^          |
                    |          v
                 logical pool mailboxes (P) ---> usable connection views

   C/P terminal records ---> native condition/waiter notification ---> API waiters
               +----------> reactor completion bridge ---> done callbacks
   C notification batches ---> callback lane ---> continuation messages
   private/health captures ---> bounded cleanup ---> reservation retirement

For this candidate, the benefit of a native C thread is independence from
reactor dispatch and application callbacks. Its cost is an additional owner
thread and cross-thread completion bridges. A process-wide shared C runner
would couple unrelated Clusters; that objection does not decide the separate
per-Cluster shared-runner option above. Evaluate the alternatives against the
same ownership, compatibility, progress and resource requirements before
recording an execution choice.

The runtime stage must provide consistent native thread/lock primitives and
a completion adapter for each supported reactor: asyncio's thread-safe loop
scheduling and the libev/asyncore loop wakeup paths. Verify those bridges
against their actual implementations; a missing bridge is an initialization
error with resource cleanup, not a silent switch to a second writer or a
blocking wait on the I/O loop.

The baseline removed the Twisted, gevent and eventlet reactors and their tests
in #969. They are outside this migration's compatibility matrix. A future
restoration of a cooperative reactor would need a separate adapter decision:
native owner primitives must remain consistent before and after monkey
patching, and wakeups and callbacks must reach the correct hub through a
verified thread-safe bridge. Saving ``threading.Thread`` alone would not
establish that contract. This design does not reinstate those reactors.

Publication retains short writer synchronization. C builds a replacement
topology view containing both UUID and endpoint indexes, ring and revision.
Ordinary routing captures an atomically published immutable view reference
without a new publication guard: ``Session._pools`` and replica-view lookups
keep their lock-free read paths. Publication guards serialize writers and
lease revocation, not every query. Multi-index management/introspection reads
may capture a coherent view under a short guard. Legacy API objects
keep their documented shape and Host identity semantics. P similarly
publishes usable connection membership. Do not expose an in-place-mutated
dict while a request reader iterates it, and do not infer safety on
free-threaded Python from reference assignment alone. Host health reads and
query-visible policy data keep the synchronization they need independently
of their single writer. Removing a lock requires the final writer/reader
audit, not just the arrival of a mailbox.

Message and result contract
---------------------------

Messages are immutable envelopes containing operation id, kind, origin,
Cluster lifetime, and the applicable identity fields below. Mutable candidate
resources have exactly one disposal owner. A worker transfers that ownership
only when its result is admitted; rejection leaves disposal with the worker.

.. list-table:: Identity and acceptance
   :header-rows: 1
   :widths: 25 43 32

   * - Result family
     - Required original identity
     - Acceptance or disposal
   * - Topology query / endpoint preparation
     - Cluster lifetime, refresh revision, original control binding and
       captured topology revision; forced-rebuild intent is tracked separately
     - C rejects superseded/incompatible observations. Preserve force intent
       until an accepted rebuild covers it. Release candidate-only resources.
   * - Host preparation / reconnect / health observation
     - UUID, exact Host, endpoint, readiness/health generation and attempt id;
       failures additionally carry original pool/connection authority
     - C rechecks all applicable fields. A retired observation cannot mark a
       replacement up/down; close an uninstalled successful reconnect socket.
   * - Pool construction / recovery
     - Session lifetime, exact Host/endpoint, pool lifecycle, intent and
       attempt id, expected installed pool, Session keyspace generation
     - C retains an already healthy matching pool. Install only current,
       usable candidates; close other candidates and settle their waiters.
   * - Policy-distance evaluation
     - Cluster/Session lifetime, exact Host/topology revision, policy revision,
       original reconciliation intent and requested keyspace generation
     - C discards a stale decision, preserving still-current reconciliation
       demand. Exceptions fail only the original operation; no candidate
       socket ownership transfers with a policy result.
   * - Shard open / replacement / USE
     - Pool lifetime, cluster-issued membership lease, shard/attempt token,
       expected slot/connection, keyspace generation
     - P rejects revoked leases/attempts. Re-USE an otherwise current candidate
       if the target changed; close obsolete sockets without touching a new slot.
   * - Pool health / retired acknowledgement
     - Exact pool/lease, Host incarnation and observation generation
     - C discounts only against current independent Session pools. A late
       acknowledgement cannot detach a newer pool with the same UUID.
   * - Pool keyspace-readiness acknowledgement (P to C)
     - Cluster and Session lifetimes, original keyspace operation/generation,
       exact pool and membership lease, Host incarnation and readiness revision
     - C checks current membership, unrevoked lease and matching keyspace
       generation before settling that operation's pool outcome. A stale ack
       cannot mark a replacement ready or satisfy a newer USE. Discard its
       value and settle only the original waiter as superseded/shutdown; P
       retains socket/stream disposal, including corrective USE if still current.
   * - Query-driven keyspace / schema continuation
     - Original quota-domain and attempt tokens, Connection/stream, exact Host,
       Session lifetime/keyspace generation and schema operation revision
     - C validates the target operation, not a fresh UUID lookup. Release the
       charged record exactly once to its captured domain after all obligations;
       stale results retain their original disposal and completion ownership.
   * - Control reconnect / schema / route refresh
     - Control generation, exact original socket and binding, operation or
       schema revision, recovery intent/attempt where relevant
     - C admits only matching current work. Close uninstalled sockets; discard
       stale metadata. Keep retry policy/backoff and finite exhaustion semantics.
   * - Timer / callback / completion delivery
     - Original operation, lifetime and generation, not a fresh lookup by UUID
     - Ignore stale wakeups; complete/cancel the original operation rather than
       transferring authority to whichever object now occupies its slot.

C issues a pool membership lease before construction. It is an immutable
identity plus a monotonic revocation cell, read with a short publication
guard. C is the sole revoker and acquires that same guard to revoke it.
P holds the guard continuously from lease validation through the member-view
update; a successful check followed by an unguarded update is insufficient.
Neither context holds it across I/O, callbacks or a cross-owner message.
P never synchronously asks C for permission. C revokes the lease before
publishing a detached pool view, so an old P cannot install new sockets in
the cross-owner message gap. Already borrowed streams retain their original
accounting references. This fence replaces cross-owner lock acquisition, not
the need to reject late asynchronous results.

Each admitted operation has an owner record and one terminal outcome:
success, failure, superseded, cancelled or shutdown. Coalesced callers attach
distinct completion waiters to the same current intent; cancelling one waiter
does not cancel other callers or physical work already in flight. A
superseded operation either follows the equivalent successor explicitly or
settles as superseded; it never silently loses its waiter. Worker submission
failure is a normal failure outcome and releases its attempt/reservation.

Timeout ends that caller's wait, not the result-disposal obligation. A running
open may finish later; its original acceptance check and cleanup still run.
Internal completion proxies support ``result``, ``done`` and
``concurrent.futures.wait``. Cancellation must notify registered waiters as
well as set a cancellation flag, preserving the #1097 regression. Their done
callbacks are dispatched outside C/P through the completion adapter. This is
an internal proxy contract; preserving query ``ResponseFuture`` outcomes is
a requirement for the query-driven admission proposal below.
Adapters must test callbacks registered both before and after terminal state.

Bounded admission and progress
------------------------------

The reservation model in this section is the worked candidate, not a settled
admission design. Its ordinary/pool/query/reentry windows, domain limits,
per-lifetime records and queue-node claims require a complete ownership and
resource audit. Accepted-operation terminal/disposal obligations and existing
public behavior remain requirements regardless of which mechanism is chosen.
Do not infer a fixed byte bound, a bound on every configuration/lifetime object,
or finite completion behind arbitrary stalled code from a record-count formula.

The proposed initial ordinary budget, ``limit``, is 1,024 outstanding
reservations per Cluster; each pool's 64-reservation limit is a subset of that
budget, not additional capacity. Count queued and running operations, coalesced
waiter records, reserved physical results and queued/active completion-delivery
batches until their obligations discharge. Native future completion alone does
not return credit while its physical cleanup or callback batch remains
outstanding.
Use at most one driver-owned completion-queue node per charged record.
These are internal limits to validate, not public configuration promises.

Capture disposal remains charged too. A private-only native record retains its
reservation until its cleanup-thread disposal finishes; native completion or
handoff to cleanup does not make that reservation reusable. This retains the
record's applicable ordinary or private admission charge, rather than
introducing an uncharged disposal queue. Report retained cleanup captures and
active disposal separately from pending operation counts; terminal operations
can still own cleanup obligations.

Ordinary internal nonblocking submissions reject immediately with an internal
``ManagementOverloaded`` outcome, distinct from shutdown. They retain no
unadmitted waiter or retry queue. Public synchronous ``Cluster.connect``,
metadata/schema refresh and ``Cluster.add_execution_profile`` wrappers do not
expose a new capacity exception. Before allocating a manager record, their calling
thread waits for ordinary admission through a native condition, without a
driver-owned pending-request list. Existing API deadlines, where defined,
cover this wait and retain their existing timeout/error contract. Calls from
the active L callback use the reentry reservation below instead. No capacity
timeout is invented for an otherwise untimed API. Close wakes these callers
through the existing shutdown outcome. Owners and reactor threads never
perform this wait. Manager-owned backlog stays bounded; blocked caller stacks
remain proportional to the application's calling threads. An otherwise
untimed caller can remain blocked until capacity returns or shutdown begins;
an application callback that never returns provides no finite admission bound.
This limitation must be documented by the runtime implementation. The separate
reactor-context compatibility proposal below applies before this wait.

For ``add_execution_profile``, first preserve validation, registration and
``populate/on_up`` ordering on the initiating stack. Do not hold the registry
guard across policy code. After policy initialization, mark one native,
versioned pool-reconciliation demand slot and wake C; C reconciles the current registered
profiles and Sessions, rather than retaining one unadmitted task per profile.
Registered profiles remain configuration data, not an admission queue. C scans
existing registries in bounded windows and charges admitted work normally.
The caller then waits for admission and its required pool readiness, using
``pool_wait_timeout`` for that wait, as the existing readiness wait does.
Timeout leaves the profile added, including when admission has not happened;
a retry still reports that it already exists. Remove the expired caller's
waiter, not its profile or the coalesced reconciliation demand. Demand remains
until C has reconciled the recorded revision; concurrent additions advance
that revision, so clearing an older demand cannot lose a registered profile.
Policy exceptions retain today's partial registration state. No owner handler
calls ``populate/on_up`` or waits for the caller. The reactor guard runs before
registration; active-L profile additions use reentry for their native phases.
That operation claims the demand through its reentry record, rather than
waiting for background ordinary reconciliation. Configuration growth remains
proportional to registered profiles and must be reported separately from the
single pending-demand slot and charged management work.

Registration makes the profile visible before its caller invokes
``populate/on_up``. Concurrent ``ProfileManager`` lifecycle delivery can
therefore reach that policy during initialization, as in the baseline. Serial
L delivery does not serialize this caller-stack phase; it is an explicit
exception to any claim that all policy lifecycle calls are serialized. This
evaluation does not introduce an initialization exclusion or move initialization
to L. Before #1259 changes this path, resolve and test the initialization/delivery
overlap while preserving registration, policy-exception and timeout behavior.

Proposed query-driven admission, requiring maintainer agreement: isolate
continuation credit by canonical Host UUID, shared by its current and retiring
incarnations, rather than impose one Cluster-wide in-flight query cap.
Initially allow 1,024 outstanding records per admitted quota domain, separate
from ordinary reservations, and retain the shared 64-slot query physical/result
window per Cluster. Conservative admission covers nearly all QUERY, EXECUTE
and BATCH attempts: the existing USE recognizer is not a DDL classifier, and
prepared statements need not reveal their effects. Unknown statements reserve
credit; only messages proven incapable of either management result, such as
protocol PREPARE, are exempt. Each retry or speculative attempt reserves
separately before its wire send, while earlier obligations remain charged.
Admission uses short native counters alongside existing connection/stream
admission, without an owner-mailbox round trip, reactor wait, pending-request
list or automatic retry queue. It does add bookkeeping to ordinary queries;
its cost and capacity effects require the hot-path trials below.

Bound quota lifetimes too: the initial proposal permits at most 1,024 owned
domains per Cluster, including removed UUIDs with debt. Allocate a domain
before publishing a previously unseen Host; endpoint replacement and re-add
of the same UUID reuse its existing domain. Removal neither returns credit nor
allocates a fresh domain. Release the domain only after all incarnations retire
and ALL transport, manager, disposal and completion obligations drain.
Every live UUID needs a domain, so this proposal also imposes a hard ceiling
of 1,024 simultaneously published live UUIDs even with no retained debt. An
initial discovery snapshot above that ceiling cannot publish. Old retirement
debt can exhaust the same limit with fewer live Hosts. Neither restriction
exists in the baseline or has compatibility approval here.
When all domains are held, retain one coalesced full-refresh demand, not a map
of unadmitted Host candidates. If a snapshot requires a new domain, defer the
whole incompatible publication, dispose its temporary candidates and retry the
coalesced refresh when capacity returns; never publish a ring with unadmitted
Hosts. Existing-domain updates that form a valid coherent batch, negative
health and close still use their reserved paths. An admitted refresh waiter stays charged and
obeys its existing deadline. An untimed refresh can wait indefinitely if debt
never drains. This topology-admission restriction does not exist today and
requires explicit compatibility review; no unchanged visibility or availability
claim is made. Before implementation, demonstrate preservation or explicitly
amend the affected requirements and review the replacement contract; approval
of host isolation alone cannot authorize this tradeoff.

When no record is available, follow the existing query-capacity failure path:
record ``NoConnectionsAvailable`` for that host or connection candidate;
normal query-plan and control-fallback selection may send only after acquiring
a record from the exact bound Host's domain. Do not create endpoint-keyed
fallback domains; an unresolved control binding cannot authorize a send.
One host's retained records do not exhaust an existing healthy host's quota.
Exhaustion reports ``NoHostAvailable`` with the existing per-host
or candidate error-map shape. An already expired deadline or shutdown retains
its existing outcome. No attempt, retry or fallback may send without credit.
This is a per-host cap on most attempts, potentially below the aggregate stream
capacity of that host's connections, and an additional pre-send failure cause.
Identical failure conditions are not claimed. Its compatibility implications
require explicit review before implementation, despite using existing exception types.

An admitted response transfers its record to C without blocking the reactor
or returning ordinary ``ManagementOverloaded`` to ``ResponseFuture``. After
admission, keyspace propagation and DDL refresh preserve their existing
success, error, schema-agreement and timeout outcomes. ``Session.set_keyspace``
uses ``execute('USE ...')`` and this same reservation, without an additional
ordinary-admission wait or reservation. Both forms therefore use the same
admission path with ordinary credits held. An L-callback ``set_keyspace`` takes
only query credit, not the synchronous reentry record. Full query credit can
still cause its specified capacity failure; L is not an exemption. Its native
completion cannot await the active L notification's acknowledgement. The
synchronous wrapper waits for the query outcome; the reactor-context proposal
below applies before its send.

Count active and application-timed-out/cancelled records together. Credit stays
charged through ALL transport, manager, result-disposal and completion
obligations; caller timeout/cancellation never permits a replacement to bypass
that debt. Use at most one queued node per record. A physical/result slot
stays charged until its work and result-disposal obligations drain; fan-out
uses the shared 64-slot window rather than an uncharged per-target work queue.
Dispatch eligible domains fairly; at most one ready link per admitted domain
is embedded in its header. Shared-worker stalls can still delay management
completion; host quota isolation guarantees admission isolation, not finite
completion behind untimed I/O or application code.
Reserve result slots before physical work so results cannot wait for ordinary
admission. Preserve Session keyspace generation and accepted schema revision.
Pool USE and schema reads needed to complete an admitted continuation use this
shared physical/result window, never another query-continuation reservation.

For D owned domains the bounds are D <= 1,024, at most ``1,024 * D`` record headers
and record/completion nodes, D embedded domain-ready links, and 64 shared work
descriptors with reserved result slots. Thus the maximum is 1,048,576 records
and nodes, not 1,024 total; no 64-per-host work allocation is introduced.
Closing the source connection does not alone discharge already handed-off
manager work. Existing stream accounting returns before asynchronous USE/schema
completion, so per-connection credits alone would not bound replacement debt.
Original query payloads/results and required per-host aggregates remain charged to their
queries; no fixed byte-size bound is promised. The domain seed covers the
proposed 1,000-host trial with limited retirement headroom; the per-host seed
uses the ordinary record scale, and 64 retains the shared work window. These
are concrete sizing hypotheses, not measured or agreed acceptable limits.
Baseline/candidate trials must measure header, aggregate and retained-payload
costs, rejection rates and latency across topology and stream sizes, including
the large aggregate maximum.
Retune domain/host/work limits to satisfy agreed workloads and memory/latency
budgets before implementation. Capacity, topology admission and error mapping
are author proposals, not agreed budgets or released behavior; compatibility
requires maintainer agreement.

Introduce and test any selected domain-publication gate in #1258, before a
new UUID can enter metadata. Keep its query/keyspace/schema integration in
#1259/#1261. Add deterministic #1259/#1261 scenarios for ``set_keyspace``,
explicit USE and DDL with ordinary credits held, then fill one host's records with paused
timeout cleanup: healthy-host attempts still send, while that host gets the
specified capacity error. Check that retries, speculative attempts and fallback
cannot bypass the cap, including reverse completion and repeated caller
timeouts with cleanup paused. Release one fully drained record and admit at
most one additional concurrent attempt; retain no driver-owned admission waiters.
Check QUERY/EXECUTE/BATCH speculation, PREPARE followed by EXECUTE, control
binding, shutdown and timeout/error precedence. Fill all domain slots, remove
and re-add UUIDs while cleanup is paused, and discover another UUID: no new
domain, candidate backlog or new-host publication until one domain fully drains;
existing healthy-host attempts still send when their stream capacity allows,
and reserved native close still runs. Check health progress with its separate
consumed and pending-disposal capacity available; test the specified deferral
or rejection separately when those windows are full. Check final
zero debt. Use small injected limits for deterministic schedules and the full
proposed limits for resource/load trials.

One additional synchronous reentry record belongs to the Cluster callback
lane, usable only by its currently active callback. It is independent of
ordinary work and notification-delivery credits, with a dedicated window of
at most 64 prerequisite physical/result records, included in reported limits.
Larger fan-outs advance in bounded windows; they never wait for ordinary
admission. Sequential connect, refresh or execution-profile additions reuse it
after their configuration/policy phase, native outcome and physical-result/disposal
completion; its private synchronous waiter has no arbitrary done-callback
batch. Its prerequisite results use the same reserved record and cannot wait
for a notification acknowledgement from that active lane. Policy evaluation
on the initiating stack happens before acquiring this record; nested policy
calls therefore finish before the outer operation is admitted. Once admitted,
its native completion path invokes no application code on the waiting stack.
Do not recursively run a second owner operation on the same record. Test
reentry with every ordinary credit held by paused notification/completion
batches. Calls into another Cluster use that Cluster's ordinary admission;
application-created cross-Cluster wait cycles remain outside this guarantee.

Coalesce refresh demand by target, missing-shard demand by pool/shard, and
recovery demand by exact current intent. Keep forced-refresh bits and distinct
charged waiter records. Do not merge opposite admitted UP/DOWN transitions
merely because UUIDs match. Bound duplicate waiters by ordinary admission.

Each current/retiring Host incarnation has one separately reserved native
health/removal record and one latest unadmitted notification slot. That
record applies negative health, request-view exclusion and lease revocation
without acquiring ordinary capacity, waiting for slow opens, or waiting for
L callback delivery. Consumed-health and pending-disposal capacity can still
defer or reject an observation as specified below; this candidate does not
guarantee DOWN/REMOVE progress through stalled capture disposal. Removal
supersedes pending positives when admitted. It does not allocate
physical work or a new callback batch: those require ordinary transition
credits. Preserve already admitted versioned batches and their waiters;
stale batches revalidate before effects. Pending unadmitted notifications
coalesce until callback credit returns, and positive readiness remains gated
on accepted pool outcomes. Previously unseen identities set one pending full
refresh demand instead of growing a key map. Record counts are bounded by
owned current/retiring incarnations; retirement frees its record after its
obligations drain. Test DOWN/REMOVE progress with all 1,024 ordinary credits
held by slow I/O and separately by paused callbacks, while reserved health
and pending-disposal capacity remain available.

A consumed health handler keeps its incarnation slot active through cleanup;
newer observations coalesce in that slot's single pending record. Unregistering
cannot free an active slot, and re-registering the same incarnation reuses it.
An old disposal must not clear a newer pending observation or create a second,
uncharged slot. Bound consumed and cleanup-pending health records across
all incarnations with a proposed separate health window. It shares the
candidate's ``limit`` seed of 1,024 but does not consume ordinary capacity.
Further pending observations coalesce until reserved health capacity returns.
A full consumed-health limit defers native application of new observations;
ordinary admission saturation alone does not.
A paired notification remains pending until its native health application
finishes, including when the active slot or aggregate health limit defers it.

Replacing or unregistering a pending observation can release its old handler
and notification captures. When either old capture is present, C/P must reserve
safe disposal before changing that observation or registration. An empty old
pair needs no disposal batch and can change despite a full disposal window.
Permit at most one pending-disposal batch per incarnation, containing at most
the old handler and notification captures, and at most ``limit`` such batches
across the Cluster. This proposed window shares
the same numerical seed, but its count is separate from consumed-health and
ordinary capacity.
These are retained-object counts, not bounds on arbitrary payload bytes.

For operations that retire an old capture, raise ``ManagementOverloaded`` if
the incarnation already owns a pending-disposal batch or the aggregate limit
is full. Raise it before changing the pending observation or registration.
Preserve the old captures and state so the producer can retry. Successful
replacement and unregistration retain their behavior.
Queue admission must also succeed before transferring captures or charging the
batch; a failed admission must leave the old state intact. Release accepted
captures on the cleanup service outside C/P and runtime/Future locks. Return
the batch's capacity only after actual disposal, including destructors and
weakref callbacks, finishes. Producers outside C/P can release replaced pending
captures directly outside runtime locks, without adding a disposal batch.

Retain an unregistered incarnation's slot until both consumed-health work and
pending-disposal batches drain; re-registration reuses that slot. This bounds
retired-incarnation churn without claiming a fixed bound on live registration
data. Ordinary saturation alone cannot block reserved health progress, but
pending-disposal capacity can reject C/P replacement or unregistration. Native
close retirement retains its guaranteed reserved path.

Reserve one result slot with each physical item before submitting it. Results
and cleanup retain that reservation, so ordinary admission cannot prevent their
return. Each Cluster, registered Session and logical pool owns one guaranteed,
idempotent close record allocated with its lifetime. Concurrent closes join
that object's record; different Sessions never compete for one Cluster-wide
close slot. ``Session.shutdown`` and ``Cluster.shutdown`` cannot fail for
capacity. Session close retires only its membership, leases and operations;
Cluster close absorbs outstanding child-close obligations. These reserved
records may retire state and revoke leases without callback capacity.

If an owner mailbox is closed, the producer/late-result sink disposes its
candidate and settles its original completion without reopening the mailbox.
No retry loop exists solely to re-enqueue an undeliverable result. Queued and
active callback batches retain charged records; a slow listener delays its
lane, not native result publication, cleanup or reentrant API completion.
Report ordinary, query-driven, health, reentry, close and delivery high-water
counts separately; never hide growth in an uncharged auxiliary queue.

The cleanup context must not wait for the capacity or disposal that it advances.
Reject blocking admission, pending ``result``/``exception`` and bulk future
waits, synchronous reentry, and cleanup-dependent joins there. Already-terminal
reads, nonblocking submissions and nonblocking close remain valid. Blocking
close uses the same cleanup-context wait guard. Test destructor and
weakref-callback reentry through these paths, without holding runtime or Future
locks across capture release. No finite completion bound is claimed for a
destructor that itself never returns.
Such a destructor can hold the record's ordinary or private reservation,
backpressure health consumption, or exhaust pending-disposal capacity. A
pending DOWN/REMOVE can then leave the old routing view visible until capacity
returns and the producer retries a rejected observation. C/P dispatch and native
close retirement remain available; finalization and joins still require actual
disposal to finish.

Decisions required before adopting the candidate
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The following are open evaluation decisions, not implementation permissions.
Record the chosen mechanism, its assumptions, compatibility consequences,
acceptance criteria and required evidence before runtime implementation starts
at #1257. Later stage entries below assign implementation and validation work;
they do not postpone the agreement on acceptable regressions required by #1256.
A numerical seed, an existing exception name or a reviewed documentation page
does not itself establish an accepted change to public behavior.

.. list-table:: Open mechanisms and stage-entry evidence
   :header-rows: 1
   :widths: 23 42 35

   * - Decision
     - Options and consequences to evaluate
     - Evidence before the affected runtime work
   * - Execution and ordinary admission
     - Compare the arrangements above. The candidate's 16-message turns,
       1,024 ordinary records, pool subset of 64, reentry window and per-lifetime
       records are concrete proposals, not universal constants. The separate
       consumed-health and pending-disposal windows each share the proposed
       ``limit`` seed; neither is an existing released-driver limit. Blocking-I/O
       executor reuse may couple owners to their prerequisites.
     - Before #1257, agree execution/admission and bridge contracts; demonstrate
       handler fairness, exclusive handoff, supported-runtime wakeups, reserved
       results/health/close and no self-wait. Retain ordinary API deadlines and
       errors, including untimed callers and profile registration after timeout.
   * - Query continuation and retained debt
     - Evaluate per-Host domains, permits tied to original transport ownership,
       or selective admission with a complete account of unknown statements.
       The existing USE recognizer does not classify arbitrary DDL. Returning
       a stream or closing its socket does not drain handed-off manager work.
     - Agree original-attempt ownership before #1257. Before relevant
       #1259/#1261 query/keyspace/schema migration, test timeout, cancellation,
       retries, speculation, fallback, disposal and replacement/re-addition.
       Test all retained cases below; do not hide debt in an unadmitted queue
       or reuse credit before its obligations discharge.
   * - Capacity and topology compatibility
     - The candidate's 1,024 records per Host can reject a healthy Host's next
       attempt while baseline connection streams remain available. Its 1,024
       retained domains can indefinitely defer a valid new-UUID topology
       snapshot while old debt remains, and impose a hard ceiling of 1,024 live
       UUIDs even without debt. These are distinct new restrictions,
       not proof of preserving baseline capacity or topology visibility.
       Stalled health capture disposal can also defer negative health or reject
       pending replacement/removal, retaining an old routing view. This is a
       proposed dependency, not proof of preserving baseline recovery.
     - Agree acceptable regressions before #1257. Before relevant #1258
       publication or #1259/#1261 admission work, demonstrate preservation of
       #1256's query hot-path rule, this document's topology invariants and
       #382's recovery requirement. A compatibility discussion alone cannot
       waive them. An incompatible proposal needs an explicit recorded amendment
       of those requirements before implementation. No such amendment is
       recorded here.
       Keep the proposed limits, full-domain churn, aggregate/payload accounting
       and healthy-host trials. Measure sizing rather than treating host
       isolation as approval of these limits.
   * - Reactor synchronous calls
     - The RuntimeError option below replaces existing possible blocking,
       timeout or deadlock on reactor callbacks. It is not an accepted error
       contract merely because native waiters and callbacks are separated.
     - Resolve compatibility before #1257; validate its affected adapters and
       #1259 APIs. Name every affected API, pre-state-change behavior and tests.
       Keep the ordinary caller contract and valid async/terminal-result paths.
   * - Cleanup-context blocking calls and close
     - The candidate rejects pending waits and blocking close with
       ``ManagementContextError`` on its cleanup thread, before a blocking
       close changes state. Terminal reads and nonblocking close remain valid.
       This is a new API restriction, including when destruction reenters an API.
     - Resolve compatibility before #1257. Destructor-driven Session/Cluster
       cleanup must initiate reserved nonblocking retirement before any wait
       guard can abort it. A swallowed exception from ``Session.__del__`` must
       not abandon pool closure. Test actual Session/pool captures and pending
       initial-connect futures in the #1259/#1262 adapters; runtime-only tests
       do not establish that integration.
   * - Combined shutdown and measurement
     - Every arrangement retains late-result disposal, idempotent close,
       original resource ownership and no self/cyclic join. Thread counts,
       workloads, thresholds and measurement procedure remain proposals.
       Decide whether normal external shutdown also waits for arbitrary capture
       disposal or leaves it to eventual finalization; no choice is accepted
       here.
     - Before #1262 deletion, cover all lifetime/partial-initialization/fork
       obligations. Before #1257 runtime implementation obtain the required
       workload/budget agreement; before #1263 acceptance record the agreed
       baseline/candidate comparison and full writer/reader audit.

Existing regressions and accepted preservation requirements remain mandatory.
Tests below that assert proposed admission/reentry/demand-slot mechanisms, a
cap, domain-deferral behavior, cleanup thread count or RuntimeError are
option-specific acceptance scenarios if that mechanism is agreed; retain
their edge cases when evaluating a replacement mechanism. This distinction does
not delete a test topic, approve a compatibility exception, or claim a compatible
bounded runtime already exists. The unique field deletion responsibilities in
the migration table remain unchanged.

Lifecycle transitions and callback ordering
-------------------------------------------

.. code-block:: text

   unseen --ADD--> admitted/pending --> prepare+pools --> ready
                         ^                  |              |
                         |               failure       current DOWN
                      current UP            v              v
                         +---------- down/recovering <------+

   current DOWN during admission/preparation --> down/recovering

   any live incarnation --REMOVE or replacement--> retired (terminal)
   same UUID/new endpoint: retire old -> publish replacement -> prepare new
   same endpoint/new UUID: retire previous UUID -> admit new UUID
   swap: validate whole batch -> revoke old leases -> publish all indexes/ring

ADD/UP workers prepare statements and construct pools; C accepts their
results before readiness. Listener ``on_up`` observes all required Session
pool results settled, including the ignored/no-pool case. Retired positive
events are skipped. ADD notification intent survives a DOWN that supersedes
preparation, so recovery does not omit the eventual discovery notification.

Preserve the baseline callback sequence in each versioned transition:

* DOWN: policy membership first, then control recovery, Session reactions,
  listeners and host-reconnect continuation. A stale generation stops later
  effects. One callback exception does not skip required retirement/cleanup.
* UP: cancel the old reconnector; evaluate distance and prepare statements;
  remove each Session's old pool before policy ``on_up``; then control reaction,
  required pool creation, accepted readiness, pending ADD and UP listeners;
  finally ``Session.update_created_pools`` reconciliation for completed pool
  attempts. The no-futures branch is distinct: UP listeners run only when
  completing a pending ADD notification, and it does not call
  ``update_created_pools``. Preserve the no-pending-discovery suppression for
  both LOCAL and IGNORED distance. Recheck authority between all applicable effects.
* REMOVE: retire/revoke first; then policy removal, Session detachment,
  listeners and control reaction, continuing cleanup after callback errors.
* Location/replacement: remove old policy membership before classifying the
  replacement, publish coherent metadata before listeners, and revalidate
  location/refresh revisions after callbacks. Preserve atomic endpoint swaps.

L-delivered policy notifications and listeners run serially outside owner
handlers and publication locks. A callback batch returns an acknowledgement carrying its
original generation; C uses it only to schedule still-current continuations.
C continues processing other observations while a callback is slow. Public
query-plan and ``distance`` methods remain callable concurrently as today;
they are not forced through this lane or the C mailbox. Custom policy state
continues to require its own thread-safety.

Synchronous API completion and reentrancy
-----------------------------------------

Public callers may block; owners may not. An API wrapper submits an operation,
then waits through the runtime adapter while its owner continues. Successful
topology refresh means accepted metadata/ring publication, not delivery of
every listener. A location change is a specific exception: refresh completion
accepts its revision and retained location intent, but excludes location/ring
publication still waiting for the old-membership policy callback. Publish that
location only after its versioned acknowledgement; a newer refresh joins or
supersedes the intent without waiting for the active callback. Other accepted
metadata/ring changes can publish normally. This preserves old-location policy
removal order without letting a reentrant ``on_down`` refresh wait on itself.
Document this deferred-location boundary in the refresh adapter and test it
with nested refresh, replacement and a later superseding revision.
While the acknowledgement is pending, callers can observe the previously
published location/ring; refresh success does not promise that this deferred
location is visible. Endpoint replacement still follows its own identity and
lease rules. Arbitrary application code has no finite completion bound, so
neither does this deferral. Expose pending deferred-location count and oldest
age in management diagnostics, and test the retained routing view while the
old-membership callback is paused. Negative-health exclusion must progress
independently of that callback while its health/disposal windows permit
admission; capture disposal can still defer or reject it as described above.
Native close retirement remains independent.

``Cluster.connect`` retains first-usable-pool completion by default and
all-initial-attempts completion for ``wait_for_all_pools=True``. Registration,
control setup and pool results have separate owner operations; constructors
must not hide a blocking wait inside a handler. ``Session.set_keyspace`` waits
for the existing USE/error contract and the accepted pool readiness outcomes,
not for unrelated future pool creations.

For ordinary C-driven ADD/UP or Session reconciliation, evaluate custom
``distance`` on L as a versioned policy batch backed by the requesting
operation's reservation. Capture topology, Session/pool intent and policy
revision; C validates the returned decision and continues asynchronously.
A slow listener can therefore delay background classification on L. That lane
does not own native negative-health/close handling; the separate capture-cleanup
capacity limits above still apply to health. L cannot consume a physical-work
worker needed by synchronous policy reentry. Do not run driver-managed lifecycle
policy callbacks on the physical-work executor, C or P. Within this worked
candidate, the fifth thread is solely the bounded capture-cleanup service;
it does not add another lifecycle policy/callback lane. A different evaluated
arrangement needs its own reviewed resource account; five threads are not a
universal ownership requirement. Physical work returns observations
instead of waiting for a management continuation that requires another task
on that same executor.

Synchronous caller and callback wrappers evaluate policy decisions on their
initiating stack between native phases, before acquiring admission. If control
setup or a newer topology requires another decision, complete that native phase
and release its record before calling policy code, then admit the next phase
with the captured decision. No phase of a callback-initiated API may queue a
required policy decision behind its own active L batch. This also permits a
policy callback's nested refresh when ``executor_threads=1``: L remains occupied,
but the physical worker is available for the refresh query/preparation.
Public query-plan/distance concurrency remains the policy's existing
responsibility. An asynchronous background continuation may wait for its
notification acknowledgement; synchronous API completion may not depend on
its own active callback's acknowledgement. Test connect, refresh, execution-profile
addition, keyspace change and shutdown from policy and listener callbacks, including one physical
worker and full ordinary delivery capacity. Do not recursively drain batches
or run blocking handlers inline on C.

The proposed reactor-context compatibility change requires maintainer
agreement before implementation. Detect supported reactor/I/O thread identities
as well as C/P context before a synchronous management wrapper allocates work
or changes state. ``Cluster.connect``, ``Cluster.refresh_nodes``, all public
``Cluster.refresh_*_metadata`` methods, ``Cluster.add_execution_profile`` and
``Session.set_keyspace`` raise the
built-in ``RuntimeError`` when invoked on a reactor/I/O thread where waiting
could block their own completion. This includes invocation from a
``ResponseFuture`` callback. It intentionally replaces today's possible
blocking, timeout or deadlock for these unsupported synchronous calls; it does
not claim unchanged error behavior for that context. Document this boundary
in each affected public API and test the exception type before any submission
or state change. Already terminal result reads and asynchronous submission
remain valid. Native condition/waiter notification is independent of reactor done-callback dispatch
and of the lifecycle lane. Test pre/post-terminal registration, cancellation,
standard future chaining and an active listener waiting for that chain.
Shutdown is the deferred-cleanup exception and never joins its own reactor.

Private owner code uses asynchronous continuations. An accidental owner-side
``result``/join on an unfinished operation fails immediately with an internal
context error, rather than deadlocking. Callback-initiated shutdown publishes
retirement and hands joins to asynchronous cleanup continuations; it cannot join its own
callback lane or execution resource.

Pool and refresh transitions
----------------------------

.. code-block:: text

   reconcile -> claim/coalesce intent -> construct candidate (worker)
                    |                         |
           newer healthy pool          candidate result
                    |                         v
                 retain <--- C validates owner/intent/keyspace/usability
                                              |
                                  install or dispose; settle waiters

   failed pool --discounted DOWN--> exact-pool recovery intent
       -> timer message -> reconcile -> completion message -> next delay
       -> replacement/removal/close/exhaustion ends this recovery intent

   refresh request -> reserve revision/force intent -> query+prepare (worker)
       -> C accept revision/binding -> publish topology+ring -> settle request
       -> stale/error: dispose result; retain unsatisfied forced rebuild intent

P follows ``slot missing -> open pending -> USE pending -> usable``. A newer
keyspace generation loops through asynchronous corrective USE before member
publication. Replacement detaches an unusable slot and retains its old
connection for draining; duplicate open/replacement demand shares one attempt.
Wrong-shard or excess connections follow existing shard-aware fallback and
bounded excess cleanup. A late USE/failure/open cannot change a revoked pool.
Keyspace replies still release their original stream before corrective USE
reserves another. C and P exchange health and keyspace acknowledgements;
neither acquires the other's state locks or waits for a reply.

Control recovery has one current intent and schedule. Duplicate errors or
heartbeats retain demand without resetting active backoff. A candidate binds
to its original socket/Host/route, and only a current attempt installs it.
Preserve finite-policy exhaustion; changing what happens after exhaustion is
`#1040 <https://github.com/scylladb/python-driver/issues/1040>`_, not #1261.

Shutdown and resource ownership
-------------------------------

.. code-block:: text

   running -> closing/admission rejected -> state retired/leases revoked
       -> waiters terminal + pools/control detached
       -> cancel timers/queued work + close detached resources
       -> late-result sink drained -> stop runners/owner/bridges -> closed

C first closes admission, retires Sessions/hosts, revokes leases and settles
accepted operations. P retires member slots and hands detached resources to
cleanup. Cancel timers and work not yet started; running workers retain the
late-result sink until they hand back or close their candidates. Keep cleanup
execution available until the last resource obligation is discharged. Only
then stop executors, owner threads and completion bridges. Repeated close is
idempotent and joins the same close record.

Start the cleanup service before the other management threads so it can dispose
captures after a later startup failure. Failed or interrupted startup must
retire every started execution resource without dropping captures under owner
locks or on the active L callback. Keep cleanup available until accepted private
and health captures drain, including accepted pending-disposal batches. Close
retains their capacity charges and capture obligations until actual disposal.
The final resource barrier covers all five candidate threads and retained
disposal obligations, including late results.

Keep native close, physical/resource cleanup and final execution-resource
retirement distinct. The candidate runtime's nonblocking close returns a shared
native closed event; that event does not certify physical or capture disposal.
Its finalization/join barrier waits for physical obligations, actual capture
disposal, all five runner exits and reactor-channel detachment. A non-returning
destructor can therefore prevent that final barrier from completing.

Normal external shutdown must preserve its existing transport/operation cleanup
and executor-wait behavior. In the baseline, ``Cluster.shutdown()`` normally
waits for its executor, so an active listener on another executor worker can
block it. The callback-origin self-join exemption depends on delivery in the
calling thread; closing the lifecycle coordinator alone does not join callbacks
and does not define the whole Cluster shutdown boundary. Moving notifications
to L must not silently change that external wait behavior. Whether
``Cluster.shutdown()`` also waits for the new capture-disposal debt is an open
adapter compatibility decision, not a guarantee established by the runtime
primitive. Leaving that debt to eventual finalization is a proposal that avoids
adding a wait on arbitrary destructors; waiting for it instead can make normal
shutdown unbounded. Include the chosen API boundary in the pre-#1257 agreement;
test shutdown with cleanup paused, then release cleanup and verify final
disposal and thread exit before integrating the affected #1259/#1262 adapters.
Retire/cancel queued notifications. Where the shutdown-origin and executor
rules permit it, an already-running callback may finish after shutdown and its
subsequent submissions are rejected. Its lane releases its execution resource
when it returns. A separate final resource barrier, used by lifecycle tests
after releasing controlled callbacks, observes that last lane exit. No implementation
can forcibly stop arbitrary non-returning application code.

Calls originating on an owner, callback lane, completion bridge or worker must
not self-join or wait on a cycle; they retire state immediately and delegate
remaining joins to cleanup continuations. This worked candidate proposes no
dedicated closer thread; its capture-cleanup service is not a general shutdown join worker and
must not wait for itself. Other arrangements must account for their cleanup
resources explicitly. In particular, destructor-driven close must initiate
guaranteed reserved nonblocking retirement before any pending-wait guard can
reject the call.
The baseline ``Session.__del__`` swallows shutdown exceptions, and
``Session.shutdown`` waits for initial-connect futures before closing pools;
inserting a cleanup guard into that path alone can abandon pool closure.
The adapters must preserve retirement with real Session/pool captures, including
partially initialized Sessions, rather than rely on the swallowed exception.
A Session close retires only its own pools and operations;
other Sessions and C remain operational. Preserve partial initialization,
interpreter-exit scheduling guards and fork safeguards; a child process must
not wait on inherited owner threads or reuse their mailbox/leases. Initialization
failure tears down every already-started resource in reverse dependency order.

Migration and deletion gates
----------------------------

Each stage audits all producers in its row and adds an owner-context assertion
at the publication boundary. An adapter may submit a message; it may not keep
the previous direct write as a fallback after submission rejection.

Deletion belongs to the target field, not every observation source. #1258
deletes all direct Host-health writes, including pool/heartbeat reports, and
owns host-driven tablet invalidation. #1259 deletes direct Session membership
and keyspace-target writes. #1260 deletes pool-local socket/shard/readiness
writes, including return/heartbeat producers; it reuses #1258's Host observation
adapter. #1261 deletes direct control recovery/schema publication, including
control-heartbeat and DDL producers, and owns schema/drop tablet invalidation.
Tablet payload writes stay on the separate cache API. #1262 owns service-stop
and combined teardown writes, not a second health/control migration. Repeated
producer names below describe distinct fields; each direct write has one
deletion stage.

.. list-table:: Ordered stages
   :header-rows: 1
   :widths: 12 48 40

   * - Issue
     - Producers and work moved
     - Delete old path only when
   * - #1257
     - Mailbox/runtime, completion adapters, reserved results, close sink,
       bounded capture disposal; lifecycle FIFO delivery adapters
     - Execution/admission/bridge and required workload/budget decisions above
       are recorded before runtime implementation. Admission, exception,
       saturation, cancellation, callback and runtime compatibility tests pass.
       No claim of ownership for unmigrated fields.
   * - #1258
     - Cluster add/up/down/remove, preparation and host retries; topology
       refresh workers/publication, endpoint/location batches, host failure
       observations, policy/listener continuations
     - Resolve affected topology-admission/visibility decisions before changing
       publication. Every topology/Host writer routes through C, including initial
       discovery. Introduce any selected domain-publication gate here, including
       no-debt bootstrap and retained-debt deferral. Replace coordinator
       transactions/flags only after reader
       and stale-result checks have equivalents.
   * - #1259
     - Session registration/close admission, pool intents/construction results,
       distance reconciliation, removal completions, discounted-DOWN timers
       and initial-connect aggregation; preserve PR #1255 and #1252/#1229 fixes
     - Resolve affected profile/query/keyspace admission, timeout/debt and
       compatibility decisions before changing those paths. Constructors cannot
       write ``_pools`` or decide host transitions;
       all waiters/resources settle. Remove overlapping Session-generation
       locks only after published-view readers and P leases are proven.
   * - #1260
     - Pool first socket, shard opens/replacements, pool-local return/heartbeat
       failure observations, keyspace replies, excess/trash cleanup and retirement
     - Verify pool leases and original stream/transport ownership before deletion.
       P is the sole membership/attempt writer. Keep stream/transport and
       publication synchronization; remove work/membership ownership locks
       only where no request reader or unmigrated producer needs them.
   * - #1261
     - Control connect/install, reconnect triggers/timers/results, control-heartbeat,
       route refresh, schema queries/publication and DDL refresh completion
     - Resolve affected query/schema continuation and capacity/compatibility
       decisions before moving publication. One C recovery intent owns
       installation and backoff. Remove competing
       control pending flags/locks only after binding, fallback, finite retry
       and separate schema/tablet cache contracts are covered.
   * - #1262
     - Combined close across Cluster, Session, C/P, control, scheduler,
       heartbeat service termination, workers, bridges and interpreter/fork handling
     - Combined lifetime/resource and partial-initialization obligations are
       covered. All close origins and late results have deterministic completion;
       remove teardown adapters only after integrated resource tests pass.
   * - #1263
     - Complete writer/reader audit, remaining adapters, documentation and
       end-to-end compatibility/performance comparison
     - No superseded mutation path remains; agreed budgets and combined
       workloads pass. Record justified remaining synchronization.

Regression map and deterministic gaps
-------------------------------------

Keep existing tests as behavioral contracts, adapting orchestration to the
new execution context rather than deleting their interleavings. Use controlled
workers, fake timers and barriers, not timing sleeps, for the new scenarios.

.. list-table:: Existing coverage and additions
   :header-rows: 1
   :widths: 18 43 39

   * - Contract / stage
     - Existing coverage
     - Deterministic gap to add
   * - Operations / #1257
     - ``tests/unit/test_lifecycle_coordination.py``: result aggregation,
       callback exceptions, unlocked delivery, close during notification;
       ``tests/unit/test_cluster.py``: #1097
       ``test_shutdown_does_not_hang_on_superseded_initial_connect`` and
       ``test_down_host_reconciliation_keeps_in_flight_add``
     - Assert the selected owner-context identity across concurrent producers; failed
       handler cannot strand next operation; fill admission while reserved
       result/close still completes; wait() sees cancellation and shutdown.
       Reenter connect/refresh/execution-profile addition/keyspace from L
       policy/listener callbacks with
       ``executor_threads=1`` and all ordinary delivery credits held; physical
       prerequisites and native completion must still progress.
       Repeat private-native calls while L remains occupied, retaining their
       reservations through cleanup and checking that capture high-water stays
       bounded. Repeat health consumption, unregister/re-register while disposal
       is paused, and verify one active slot plus its coalesced pending record.
       Churn distinct retired incarnations with small injected aggregate health
       limits; consumed captures and tombstones must stay within that limit.
       Hold pending-capture disposal while C/P repeatedly replaces or
       unregisters observations. Check the per-incarnation batch and separate
       aggregate bounds, overload before any state change when old captures
       exist, empty-pair success at a full quota, successful producer retry
       after capacity returns, queue-admission failure and shutdown debt.
       Pause cleanup while delivering DOWN/REMOVE: distinguish ordinary-only
       saturation from consumed-window deferral and pending-disposal rejection;
       observe the retained routing view and eventual application after retry.
       Check destructor/weakref reentry, terminal reads, nonblocking submission
       and close, and reject self-dependent waits on cleanup. Inject failure at
       each of the five thread-start phases and verify final capture/thread
       retirement, including shutdown with cleanup paused and late results.
       Profile timeout leaves it registered; retry reports already-exists.
       Test caller initialization overlapping lifecycle delivery without claiming
       that baseline registration excludes the policy from L. For the worked
       candidate, timeout before admission leaves its single revisioned
       reconciliation demand live; concurrent additions and timeout/shutdown
       must not lose profiles or grow an unadmitted task/waiter list. Its L
       profile addition uses reentry, and its L keyspace change uses query credit
       without also taking reentry.
       If the reactor-context proposal is approved, assert ``RuntimeError``
       from every affected synchronous API, including
       ``Cluster.add_execution_profile`` and ``Session.set_keyspace``, in a
       reactor callback before work is allocated or state changes. Check profile
       addition's admission/readiness wait against ``pool_wait_timeout`` and preserve ordinary
       caller validation errors; retain valid asynchronous and terminal-result paths.
   * - Topology / #1258
     - Same module: older refresh rejection, forced rebuild carryover,
       reentrant removal/refresh, policy replacement, readiness and shutdown;
       ``test_metadata.py`` atomic endpoint-chain/swap/index tests;
       ``test_cluster.py::test_pool_creation_cannot_publish_after_host_removal``
       and replacement-recovery callback/supersession tests
     - Pause a query and a callback while newer C messages publish; deliver
       results backwards, including failed later force; verify every reader
       sees a coherent index/ring view and callbacks preserve order. If domains
       are selected, test initial discovery at and above the domain limit with
       no debt, plus full-domain retirement/re-add/new-UUID deferral with paused
       cleanup. #1258 owns that publication gate; #1259/#1261 extend its tests
       through query/keyspace/schema admission and completion.
   * - Session pools / #1259
     - ``test_pool_metadata_ownership.py`` covers displacement during distance,
       construction and keyspace sync; ``test_host_connection_pool.py`` covers
       discounted-DOWN recovery; landed prerequisite PR #1255 adds FIFO
       recovery interleavings. ``test_response_future.py`` already covers
       ``test_timeout_returns_stream_to_borrowed_pool_after_replacement`` and
       ``test_admitted_request_completes_through_original_pool_after_endpoint_move``
     - Overlap two candidate constructors with an accepted healthy pool;
       count statement dispatches for the remaining #317 gap while preserving
       already-covered original-pool stream return. Close one of two Sessions
       while its candidate and timer complete. Test consistency-default writes
       on connect and profile addition when ``metadata.dbaas`` is true, including
       non-explicit profiles subsequently assigned by the application; verify
       false leaves those profiles and the Session default unchanged.
       Drop the last Session reference on cleanup with initial-connect futures
       pending; guarded waits must not prevent reserved pool retirement.
   * - Pool members / #1260
     - ``test_host_connection_pool.py``: coalesced shard recovery, submission
       rejection, draining/orphaned streams, stale replacement and USE races;
       lifecycle tests exercise pending-USE shutdown and real connection work
     - Revoke a lease before its retirement message reaches P; then finish an
       open/USE/failure. Test fairness with many ready pool mailboxes and
       saturated I/O workers; no stale slot may publish.
   * - Control/schema / #1261
     - ``test_control_connection.py`` control recovery/binding tests;
       ``test_client_route_topology.py`` and ``test_topology_routing.py``;
       schema/tablet tests in ``test_metadata.py`` and ``test_tablets.py``
     - Reverse reconnect success/failure, duplicate heartbeat during backoff,
       finite exhaustion, slow schema agreement, DDL refresh racing a drop,
       and tablet payload racing invalidation; test callback API reentrancy.
   * - Close / #1262
     - Lifecycle close-from-listener, late pool/refresh completion,
       detached reconnector and shutdown-during-USE cases
     - Close from each owner/worker/callback/bridge, during every initialization
       phase and with full mailboxes; assert terminal waiters and exact resource
       disposition. Repeat create/connect/close and test a surviving Session.
   * - Combined / #1263
     - ``tests/integration/long/test_topology_change.py`` plus standard routing,
       tablet, schema and client-route suites; #921 closure's live coverage
     - Concurrent bootstrap under traffic, down/up, replacement/endpoint moves,
       control reconnect and shard recovery together across multiple Sessions;
       track dispatch counts, convergence and leaked resources.

Runtime compatibility and performance gate
------------------------------------------

The proposed workload set, limits and measurement procedure require recorded
maintainer agreement before #1257 runtime implementation; the pre-migration
commit is pinned above. This document does not record that agreement.
Record measurements from that pinned commit before the #1263 comparison;
measuring the baseline later does not permit replacing it with migrated code.
Do not reinterpret proposed thresholds as measurements. If a budget is
unrealistic, revise this evaluation through review before accepting the runtime;
do not silently drop a workload or widen a threshold after observing failure.

Run focused manager/ownership tests and the unit suite with asyncio against
rebuilt Cython and pure-Python modules. Run libev and asyncore compatibility
tests on their supported Python/platforms, and PyPy's pure-Python path where
supported. Asyncore is limited to the Python versions supported by current CI.
Free-threaded 3.14t/3.15t CI cells are disabled pending
`#718 <https://github.com/scylladb/python-driver/issues/718>`_; do not claim
existing free-threaded coverage. Exercise a restored supported cell separately:
owner identity does not remove reader publication requirements. Pin versions and unsupported matrix
cells to the repository CI configuration, not an assumed universal reactor
matrix. Use ``EVENT_LOOP_MANAGER`` and the build controls documented in
``CONTRIBUTING.rst``; prove which module implementation was loaded.

Live validation uses the existing ``SCYLLA_VERSION``/``CASSANDRA_VERSION`` and
``PROTOCOL_VERSION`` gates documented in ``CONTRIBUTING.rst``. Explicitly
prepared clusters use the integration harness's actual ``CLUSTER_IP`` and
``KEEP_TEST_CLUSTER`` controls. Include the stable Scylla CI version, selected Cassandra
compatibility version, multi-Session and remote/ignored-host policies, keyspace
changes, TCP/Unix/SNI/client routes and advanced shard-aware fallback. Long
recovery/performance runs stay opt-in; no unconditional cluster startup is
added to ordinary unit collection.

For performance, compare baseline ``175493ae`` (including PR #1255 recovery) and
candidate on the same isolated hardware, server topology, Python/reactor,
build mode, compression, TLS and request settings. Build the candidate from
that same runtime base plus the manager changes. Pin ordinary and serial
consistency explicitly on both sides and verify effective QUERY/EXECUTE/BATCH
wire parameters; do not compare old LOCAL_ONE/unset defaults with the new
LOCAL_QUORUM/LOCAL_SERIAL defaults. Match statement preparation, successful
rebinds, routing-key-cache warm-up and speculative policy. Record CPU allocation,
node/shard counts, row/payload sizes, warm-up, sample count and raw results.
Alternate baseline/candidate order over at least five runs after warm-up;
report medians and dispersion, plus per-run p95/p99 latency. Use both a single
partition and a wide partition-key distribution, one and eight Sessions,
small requests and 64-KiB payloads, and fixed in-flight levels 1, 32 and 256.
Also offer 1,024 and 2,048 attempts per host using enough connections for the
baseline stream capacity, with QUERY, prepared EXECUTE and idempotent BATCH.
Pause one host's timeout cleanup while others serve traffic; measure per-host
admissions, errors and latency, plus global headers/nodes/payload high-water.
Exercise fake-I/O topologies with 1, 100 and 1,000 hosts and full-domain
retirement/churn, plus the live topology's actual host count. If domains are
selected, also test no-debt initial discovery at the selected domain limit and
one above it (initially 1,024 and 1,025 live UUIDs). Measure the new
topology-admission delay, including deliberately exhausted domains.
Do not combine runs with materially different environments into one ratio.

.. list-table:: Proposed acceptance budgets, pending agreement
   :header-rows: 1
   :widths: 28 39 33

   * - Workload / metric
     - Measurement
     - Budget
   * - Steady-state prepared reads/writes; token/tablet and shard routing
     - Same offered load below saturation and maximum sustained throughput;
       client CPU/request and latency distributions
     - Median throughput loss <= 5%, CPU/request increase <= 5%, p99 latency
       increase <= 10%; zero added C/P mailbox hops per ordinary request
   * - Bootstrap and recovery under traffic
     - First/all-pool connect time, down/up convergence, endpoint replacement,
       control reconnect and missing-shard recovery after controlled fault
     - Median convergence time increase <= 10%; no lost accepted operation,
       local duplicate dispatch, stale installation or retry-backoff reset
   * - Slow workers/callbacks and event bursts
     - Pause one worker/callback, submit ten times ordinary admission capacity,
       then release it; record queue/reservation high-water and rejection count
     - Outstanding reservations stay within selected limits; reserved results
       and close progress; all accepted waiters settle after dependencies run
   * - Many hosts/pools/shards
     - 1, 100 and 1,000 logical pools with fake I/O, varying shards per pool;
       also measure the live topology's resource counts
     - For the worked candidate, new management threads <= 5 per Cluster on
       threaded reactors (one C, two P runners shared within that Cluster, one
       callback lane and one capture-cleanup thread), independent of pools and
       shards; completion adapters must not add one thread per mailbox
   * - Repeated close and late results
     - 100 create/connect/close cycles; barrier-controlled late socket/pool
       completions and canceled timers, with bounded existing I/O timeouts
     - Zero residual owned connections, manager threads, outstanding operation
       records or retry timers after final resource barrier; no self/cyclic wait

The worked candidate's proposed five-thread budget excludes existing
scheduler/heartbeat/I/O executor threads, which must be reported too so a
resource transfer cannot hide growth.
Its native completion bridges must fit that proposal or obtain a reviewed
revision. Other evaluated arrangements need explicit total-resource proposals
and the same compatibility/cleanup evidence; this row does not select an engine.
Integration shutdown bounds must include configured connect/query timeouts;
unit shutdown bounds use controlled completions, not arbitrary network timing.
Workload/budget agreement and the recorded baseline comparison are required
evidence at their respective gates. Both agreement and benchmark results
remain outstanding.

Adding a management operation
-----------------------------

Choose its sole writer from the inventory, list its request-path readers,
and identify the publication guard. Specify the immutable envelope, admission
reservation, coalescing key, every terminal outcome and ownership transfer for
each candidate resource. Enumerate late result, cancellation, timeout,
submission rejection and close behavior before adding the producer. Keep I/O,
policy/listener execution and waits outside the handler. Add controlled tests
for supersession and disposal, audit all old writers, and remove their direct
mutation only after those tests pass. Update this inventory and the regression
map when the final ownership boundary changes.
