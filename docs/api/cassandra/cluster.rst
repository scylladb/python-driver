cassandra.cluster
=================

Clusters and Sessions

.. module:: cassandra.cluster

.. autoclass:: Cluster ([contact_points=('127.0.0.1',)][, port=9042][, executor_threads=2], **attr_kwargs)

   .. autoattribute:: contact_points

   .. autoattribute:: port

   .. autoattribute:: cql_version

   .. autoattribute:: protocol_version

   .. autoattribute:: compression

   .. autoattribute:: auth_provider

   .. autoattribute:: load_balancing_policy

   .. autoattribute:: reconnection_policy

   .. autoattribute:: default_retry_policy
      :annotation: = <cassandra.policies.RetryPolicy object>

   .. autoattribute:: conviction_policy_factory

   .. autoattribute:: address_translator

   .. autoattribute:: session_id

   .. autoattribute:: driver_config_reporting_enabled
      :annotation: = True

   .. autoattribute:: metrics_enabled

   .. autoattribute:: metrics

   .. autoattribute:: ssl_context

   .. autoattribute:: ssl_options

   .. autoattribute:: ssl_session_cache

   .. autoattribute:: sockopts

   .. autoattribute:: max_schema_agreement_wait

   .. autoattribute:: metadata

   .. autoattribute:: connection_class

   .. autoattribute:: control_connection_timeout

   .. autoattribute:: allow_control_connection_query_fallback

   .. autoattribute:: idle_heartbeat_interval

   .. autoattribute:: idle_heartbeat_timeout

   .. autoattribute:: schema_event_refresh_window

   .. autoattribute:: topology_event_refresh_window

   .. autoattribute:: status_event_refresh_window

   .. autoattribute:: prepare_on_all_hosts

   .. autoattribute:: reprepare_on_up

   .. autoattribute:: connect_timeout

   .. autoattribute:: schema_metadata_enabled
      :annotation: = True

   .. autoattribute:: token_metadata_enabled
      :annotation: = True

   .. autoattribute:: timestamp_generator

   .. autoattribute:: endpoint_factory

   .. autoattribute:: cloud

   .. automethod:: connect

   .. automethod:: shutdown

   .. automethod:: register_user_type

   .. automethod:: register_listener

   .. automethod:: unregister_listener

   .. automethod:: add_execution_profile

   .. automethod:: get_control_connection_host

   .. automethod:: refresh_schema_metadata

   .. automethod:: refresh_keyspace_metadata

   .. automethod:: refresh_table_metadata

   .. automethod:: refresh_user_type_metadata

   .. automethod:: refresh_user_function_metadata

   .. automethod:: refresh_user_aggregate_metadata

   .. automethod:: refresh_nodes

   .. automethod:: set_meta_refresh_enabled

.. autoclass:: ControlConnectionQueryFallback
   :members:

.. autoclass:: ExecutionProfile (load_balancing_policy=<object object>, retry_policy=None, consistency_level=ConsistencyLevel.LOCAL_ONE, serial_consistency_level=None, request_timeout=10.0, row_factory=<function named_tuple_factory>, speculative_execution_policy=None)
   :members:
   :exclude-members: consistency_level

   .. autoattribute:: consistency_level
      :annotation: = LOCAL_ONE

.. autoclass:: GraphExecutionProfile (load_balancing_policy=_NOT_SET, retry_policy=None, consistency_level=ConsistencyLevel.LOCAL_ONE, serial_consistency_level=None, request_timeout=30.0, row_factory=None, graph_options=None, continuous_paging_options=_NOT_SET)
   :members:

.. autoclass:: GraphAnalyticsExecutionProfile (load_balancing_policy=None, retry_policy=None, consistency_level=ConsistencyLevel.LOCAL_ONE, serial_consistency_level=None, request_timeout=3600. * 24. * 7., row_factory=None, graph_options=None)
   :members:

.. autodata:: EXEC_PROFILE_DEFAULT
   :annotation:

.. autodata:: EXEC_PROFILE_GRAPH_DEFAULT
   :annotation:

.. autodata:: EXEC_PROFILE_GRAPH_SYSTEM_DEFAULT
   :annotation:

.. autodata:: EXEC_PROFILE_GRAPH_ANALYTICS_DEFAULT
   :annotation:

.. autoclass:: Session ()

   .. autoattribute:: default_timeout
      :annotation: = 10.0

   .. autoattribute:: default_consistency_level
      :annotation: = LOCAL_ONE

   .. autoattribute:: default_serial_consistency_level
      :annotation: = None

   .. autoattribute:: row_factory
      :annotation: = <function named_tuple_factory>

   .. autoattribute:: default_fetch_size

   .. autoattribute:: use_client_timestamp

   .. autoattribute:: timestamp_generator

   .. autoattribute:: encoder

   .. autoattribute:: client_protocol_handler

   .. automethod:: execute(statement[, parameters][, timeout][, trace][, custom_payload][, paging_state][, host][, execute_as])

   .. automethod:: execute_async(statement[, parameters][, trace][, custom_payload][, paging_state][, host][, execute_as])

   .. automethod:: execute_concurrent(statements_and_parameters[, concurrency][, raise_on_first_error][, results_generator][, execution_profile])

   .. automethod:: execute_concurrent_with_args(statement, parameters, *args, **kwargs)

   .. automethod:: execute_concurrent_async(statements_and_parameters[, concurrency][, raise_on_first_error][, execution_profile])

   .. automethod:: execute_graph(statement[, parameters][, trace][, execution_profile=EXEC_PROFILE_GRAPH_DEFAULT][, execute_as])

   .. automethod:: execute_graph_async(statement[, parameters][, trace][, execution_profile=EXEC_PROFILE_GRAPH_DEFAULT][, execute_as])

   .. automethod:: prepare(statement)

   .. automethod:: shutdown()

   .. automethod:: set_keyspace(keyspace)

   .. automethod:: wait_for_schema_agreement

   .. automethod:: get_execution_profile

   .. automethod:: execution_profile_clone_update

   .. automethod:: add_request_init_listener

   .. automethod:: remove_request_init_listener

.. autoclass:: ResponseFuture ()

   .. autoattribute:: query

   .. automethod:: result()

   .. automethod:: get_query_trace()

   .. automethod:: get_all_query_traces()

   .. autoattribute:: custom_payload()

   .. autoattribute:: is_schema_agreed

   .. autoattribute:: has_more_pages

   .. autoattribute:: warnings

   .. automethod:: start_fetching_next_page()

   .. automethod:: add_callback(fn, *args, **kwargs)

   .. automethod:: add_errback(fn, *args, **kwargs)

   .. automethod:: add_callbacks(callback, errback, callback_args=(), callback_kwargs=None, errback_args=(), errback_kwargs=None)

.. autoclass:: ResultSet ()
   :members:

.. autoexception:: QueryExhausted ()

.. autoexception:: NoHostAvailable ()
   :members:

.. autoexception:: UserTypeDoesNotExist ()

Topology publication and lifecycle notifications
------------------------------------------------

:meth:`Cluster.refresh_nodes` synchronously publishes node-list and token
metadata. DC/rack attribute changes are delivered with their policy transition
(remove from the old location, update the Host, add at the new location), and
can lag node-list publication while a notification is running. The method does
not wait for an already-running lifecycle notification.
Policies, session pool management and registered host listeners receive queued
notifications in order, with no topology or host callback lock held while they
run. A replacement's removal notifications precede its addition notifications.

A listener can call ``refresh_nodes()`` synchronously, including while another
thread removes its host. The nested refresh publishes metadata and appends its
notifications; it does not recursively invoke a replacement's addition inside
an unfinished removal callback. Consequently metadata can be newer than the
notifications currently being delivered, and pool availability can lag metadata
publication. A newly published host remains reserved while its addition
operation owns recovery. If UP supersedes an unfinished or failed ADD, a
successful UP delivers the pending listener ``on_add`` once before ``on_up``.
It does not repeat the policy or control-connection ADD callbacks.

For ordinary ADD, policy and control-connection callback failures are logged
without automatic callback retries. They release the recovery reservation;
the pending listener discovery notification is retained for a later successful
ADD or UP. Partially applied callback effects are not rolled back. Transport
and preparation retries continue, and replacement ADD retains its existing
callback retry behavior.

Retirement is terminal for a Host object. An already-running callback may
finish after retirement, but queued stale UP, DOWN and ADD work is discarded.
A callback must return before later notifications can run; do not wait for
another lifecycle notification from the same cluster. Callback exceptions are
logged and do not strand the notification queue.

Removal callback exceptions are logged and cleanup continues for the remaining
policies, pools and listeners. The ``suppress_callback_errors`` argument to
``Cluster.on_remove()`` and ``Cluster.remove_host()`` is accepted for
compatibility but has no effect, even when ``False``. Queued removal
notifications cannot propagate exceptions to the initiating caller.

Topology queries can overlap. A snapshot older than a newer accepted refresh
is discarded instead of overwriting the newer topology. This orders client
refresh requests; it does not establish a server-side topology version.

Shutdown first closes lifecycle admission and discards queued notifications.
This coordinator step does not wait for an active listener. An external
``shutdown()`` call still waits for executor workers during executor teardown;
a listener calling it from an executor worker skips that wait to avoid joining
itself. Already-admitted user
callbacks may finish, but later notifications, refresh publication and late
pool-completion callbacks cannot restart the closed lifecycle. Pending addition
and location reservations are cleared, and current or retired host reconnectors
are cancelled.
