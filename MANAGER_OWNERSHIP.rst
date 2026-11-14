.. _manager-ownership:

Cluster and pool manager ownership
==================================

This is the proposed development contract for `issue #1256
<https://github.com/scylladb/python-driver/issues/1256>`_, the design stage of
`#382 <https://github.com/scylladb/python-driver/issues/382>`_. It describes
the target architecture, not the behavior of a released manager runtime.
No new execution context is introduced by this document. The workload set,
budgets and measurement procedure were agreed before runtime implementation.
The architecture remains subject to design review. Agreement is not a benchmark
result or a claim that the runtime has met the budgets.

Baseline and invariants
-----------------------

The source audit uses master ``b4aa97b5cace48f9e97a195a8601ac427304173d``.
`PR #1255 <https://github.com/scylladb/python-driver/pull/1255>`_ at
``9b000a4db105e785cb304a92b23beb14587358f0`` is a pending prerequisite,
not behavior or coverage already present on master. Its recovery delta is
audited separately below. Preserve that commit as the pre-migration comparison
baseline. If the prerequisite is rebased or squashed, record its landed SHA
and verify equivalent recovery behavior before comparing; do not substitute
a migrated tree. Recheck this inventory when either source changes.
The completed `#921 <https://github.com/scylladb/python-driver/issues/921>`_
established host-id identity, immutable endpoints, replacement transactions,
generation fences and regression coverage. It explicitly left the broader
single-owner redesign to #382. Its closure identifies the implementation in
PRs #1006, #1036, #1166 and #1167 and live topology/routing coverage in PRs
#1181, #1183, #1184 and #1217.

``LifecycleCoordinator`` in ``cassandra/_lifecycle.py`` currently serializes
notification delivery, but drains on submitting or executor threads. Its
transactions, Host locks and Session locks still coordinate multiple writers.
Pending PR #1255 moves discounted-DOWN recovery admission, timers and completion
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
the cluster owner; ``P`` denotes the logical owner of one pool. A worker may
own unpublished candidate data, but may not publish it into these fields.
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
       pending PR #1255 queues each continuation through lifecycle delivery
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

Select one reactor-independent native cluster-owner thread per Cluster and
logical serial pool mailboxes on a fixed management executor owned by that
Cluster. Two runners are shared only among that Cluster's pools, not across
Clusters and not one thread per pool or shard. Cluster shutdown stops only its
own runners after their obligations drain. Fork children reject inherited
runners/mailboxes and initialize independent resources. These
runners are separate from the existing executor used for blocking opens,
queries, schema agreement and cleanup. A runner processes at most 16 messages
before yielding its mailbox back to the ready queue. At most one runner owns
a given mailbox at a time, including when resubmission fails.

Use a serial callback lane per Cluster for lifecycle policy notifications and
listeners, using one callback thread on the supported reactors. Future
completion is a separate delivery path and cannot queue
behind a listener that is waiting for that future. No owner invokes arbitrary
future callbacks as part of a state transition.

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

Ordinary threading, a reactor task and a shared serial executor were compared:

* A native C thread keeps ownership independent of an application event loop
  and avoids blocking asyncio/libev/asyncore dispatch with management work.
  Its cost is one owner thread and a required cross-thread completion bridge.
* A task per asyncio reactor would give a natural loop bridge but would require
  different ownership engines for libev and asyncore, and
  would depend on loop lifetime outside Cluster ownership. Do not select it.
* A shared C runner reduces idle threads but permits starvation by an unrelated
  Cluster and complicates synchronous initialization/close. P runners are shared
  within one Cluster only; their handlers cannot block and have explicit fairness.
  No cross-Cluster runner starvation is introduced.

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
an internal proxy contract; query ``ResponseFuture`` semantics are unchanged.
Adapters must test callbacks registered both before and after terminal state.

Bounded admission and progress
------------------------------

The initial ordinary budget is 1,024 outstanding reservations per Cluster;
each pool's 64-reservation limit is a subset of that budget, not additional
capacity. Count queued and running operations, coalesced waiter records,
reserved physical results and queued/active completion-delivery batches until
their obligations discharge. Native future completion alone does not return
credit while its physical cleanup or callback batch remains outstanding.
Use at most one driver-owned completion-queue node per charged record.
These are internal limits to validate, not public configuration promises.

Internal nonblocking submissions reject immediately with an internal
``ManagementOverloaded`` outcome, distinct from shutdown. They retain no
unadmitted waiter or retry queue. Public synchronous ``Cluster.connect``,
metadata/schema refresh and ``Session.set_keyspace`` wrappers do not expose a
new capacity exception. Before allocating a manager record, their calling
thread waits for ordinary admission through a native condition, without a
driver-owned pending-request list. Existing API deadlines, where defined,
cover this wait and retain their existing timeout/error contract; no capacity
timeout is invented for an otherwise untimed API. Close wakes these callers
through the existing shutdown outcome. Owners and reactor threads never
perform this wait. Manager-owned backlog stays bounded; blocked caller stacks
remain proportional to the application's calling threads.

One additional synchronous reentry record belongs to the Cluster callback
lane, usable only by its currently active callback. It is independent of
ordinary work and notification-delivery credits, with a dedicated window of
at most 64 prerequisite physical/result records, included in reported limits.
Larger fan-outs advance in bounded windows; they never wait for ordinary
admission. Sequential connect, refresh
or keyspace calls reuse it after native outcome and physical-result/disposal
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
callback delivery. Removal supersedes pending positives. It does not allocate
physical work or a new callback batch: those require ordinary transition
credits. Preserve already admitted versioned batches and their waiters;
stale batches revalidate before effects. Pending unadmitted notifications
coalesce until callback credit returns, and positive readiness remains gated
on accepted pool outcomes. Previously unseen identities set one pending full
refresh demand instead of growing a key map. Record counts are bounded by
owned current/retiring incarnations; retirement frees its record after its
obligations drain. Test DOWN/REMOVE progress with all 1,024 ordinary credits
held by slow I/O and separately by paused callbacks.

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
Report ordinary, health, reentry, close and delivery high-water counts
separately; never hide growth in an uncharged auxiliary queue.

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

Policy notifications and listeners run serially outside owner handlers and
publication locks. A callback batch returns an acknowledgement carrying its
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
A slow listener can therefore delay background classification on L. It cannot
delay native negative-health/close handling or consume a physical-work worker
needed by synchronous policy reentry. Do not run driver-managed lifecycle
policy callbacks on the physical-work executor, C or P, and do not add a fifth
management lane. Physical work returns observations instead of waiting for a
management continuation that requires another task on that same executor.

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
its own active callback's acknowledgement. Test connect, refresh, keyspace
change and shutdown from policy and listener callbacks, including one physical
worker and full ordinary delivery capacity. Do not recursively drain batches
or run blocking handlers inline on C.

Detect supported reactor/I/O thread identities as well as C/P context before
a synchronous management wrapper allocates work or changes state. A call that
could block its own completion/I/O loop fails immediately with an internal
context error (including calls from ``ResponseFuture`` callbacks); already
terminal result reads and asynchronous submission remain valid. Native
condition/waiter notification is independent of reactor done-callback dispatch
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

Normal external shutdown waits for its transport/operation cleanup barrier,
but does not wait for an active application notification to return, preserving
the existing coordinator-close contract. Retire/cancel queued notifications;
an already-running callback may finish after shutdown and its subsequent
submissions are rejected. Its lane releases its execution resource when it
returns. A separate final resource barrier, used by lifecycle tests after
releasing controlled callbacks, observes that last lane exit. No implementation
can forcibly stop arbitrary non-returning application code.

Calls originating
on an owner, callback lane, completion bridge or worker must not self-join or
wait on a cycle; they retire state immediately and delegate remaining joins
to cleanup continuations, without adding a dedicated closer thread.
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
     - Mailbox/runtime, completion adapters, reserved results, close sink;
       lifecycle FIFO delivery adapters
     - Admission, exception, saturation, cancellation, callback and runtime
       compatibility tests pass. No claim of ownership for unmigrated fields.
   * - #1258
     - Cluster add/up/down/remove, preparation and host retries; topology
       refresh workers/publication, endpoint/location batches, host failure
       observations, policy/listener continuations
     - Every topology/Host writer routes through C, including initial
       discovery. Replace coordinator transactions/flags only after reader
       and stale-result checks have equivalents.
   * - #1259
     - Session registration/close admission, pool intents/construction results,
       distance reconciliation, removal completions, discounted-DOWN timers
       and initial-connect aggregation; preserve PR #1255 and #1252/#1229 fixes
     - Constructors cannot write ``_pools`` or decide host transitions;
       all waiters/resources settle. Remove overlapping Session-generation
       locks only after published-view readers and P leases are proven.
   * - #1260
     - Pool first socket, shard opens/replacements, pool-local return/heartbeat
       failure observations, keyspace replies, excess/trash cleanup and retirement
     - P is the sole membership/attempt writer. Keep stream/transport and
       publication synchronization; remove work/membership ownership locks
       only where no request reader or unmigrated producer needs them.
   * - #1261
     - Control connect/install, reconnect triggers/timers/results, control-heartbeat,
       route refresh, schema queries/publication and DDL refresh completion
     - One C recovery intent owns installation and backoff. Remove competing
       control pending flags/locks only after binding, fallback, finite retry
       and separate schema/tablet cache contracts are covered.
   * - #1262
     - Combined close across Cluster, Session, C/P, control, scheduler,
       heartbeat service termination, workers, bridges and interpreter/fork handling
     - All close origins and late results have deterministic completion;
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
     - Assert native owner identity across concurrent producers; failed
       handler cannot strand next operation; fill admission while reserved
       result/close still completes; wait() sees cancellation and shutdown.
       Reenter connect/refresh/keyspace from L policy/listener callbacks with
       ``executor_threads=1`` and all ordinary delivery credits held; physical
       prerequisites and native completion must still progress.
   * - Topology / #1258
     - Same module: older refresh rejection, forced rebuild carryover,
       reentrant removal/refresh, policy replacement, readiness and shutdown;
       ``test_metadata.py`` atomic endpoint-chain/swap/index tests;
       ``test_cluster.py::test_pool_creation_cannot_publish_after_host_removal``
       and replacement-recovery callback/supersession tests
     - Pause a query and a callback while newer C messages publish; deliver
       results backwards, including failed later force; verify every reader
       sees a coherent index/ring view and callbacks preserve order.
   * - Session pools / #1259
     - ``test_pool_metadata_ownership.py`` covers displacement during distance,
       construction and keyspace sync; ``test_host_connection_pool.py`` covers
       discounted-DOWN recovery; pending prerequisite PR #1255 adds FIFO
       recovery interleavings. ``test_response_future.py`` already covers
       ``test_timeout_returns_stream_to_borrowed_pool_after_replacement`` and
       ``test_admitted_request_completes_through_original_pool_after_endpoint_move``
     - Overlap two candidate constructors with an accepted healthy pool;
       count statement dispatches for the remaining #317 gap while preserving
       already-covered original-pool stream return. Close one of two Sessions
       while its candidate and timer complete.
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

The workload set, limits and measurement procedure were agreed before #1257
runtime implementation; the pre-migration commit is pinned above.
Record measurements from that pinned commit before the #1263 comparison;
measuring the baseline later does not permit replacing it with migrated code.
Do not reinterpret the agreed thresholds as measurements. If a budget is
unrealistic, revise this contract through review before accepting the runtime;
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

For performance, compare baseline (including PR #1255 recovery behavior) and
candidate on the same isolated hardware, server topology, Python/reactor,
build mode, compression, TLS and request settings. Record CPU allocation,
node/shard counts, row/payload sizes, warm-up, sample count and raw results.
Alternate baseline/candidate order over at least five runs after warm-up;
report medians and dispersion, plus per-run p95/p99 latency. Use both a single
partition and a wide partition-key distribution, one and eight Sessions,
small requests and 64-KiB payloads, and fixed in-flight levels 1, 32 and 256.
Do not combine runs with materially different environments into one ratio.

.. list-table:: Agreed acceptance budgets
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
     - New management threads <= 4 per Cluster on threaded reactors (one C,
       two P runners shared within that Cluster, one callback lane), independent of pools/shards;
       completion adapters must not add one thread per mailbox
   * - Repeated close and late results
     - 100 create/connect/close cycles; barrier-controlled late socket/pool
       completions and canceled timers, with bounded existing I/O timeouts
     - Zero residual owned connections, manager threads, outstanding operation
       records or retry timers after final resource barrier; no self/cyclic wait

The four-thread budget excludes existing scheduler/heartbeat/I/O executor
threads, which must be reported too so a resource transfer cannot hide growth.
Native completion bridges must fit that budget or obtain a reviewed revision.
Integration shutdown bounds must include configured connect/query timeouts;
unit shutdown bounds use controlled completions, not arbitrary network timing.
Workload/budget agreement and the recorded baseline comparison are required
evidence at their respective gates. Agreement is recorded; benchmark success
remains unproven.

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
