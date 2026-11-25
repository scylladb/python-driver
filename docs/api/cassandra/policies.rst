cassandra.policies
==================

Load balancing and Failure Handling Policies

.. module:: cassandra.policies

Load Balancing
--------------

.. autoclass:: HostDistance
    :members:

.. autoclass:: LoadBalancingPolicy
   :members:

.. autoclass:: RoundRobinPolicy
   :members:

.. autoclass:: DCAwareRoundRobinPolicy
   :members:

.. autoclass:: RackAwareRoundRobinPolicy
   :members:

.. autoclass:: WhiteListRoundRobinPolicy
   :members:

.. autoclass:: TokenAwarePolicy
   :members:

.. autoclass:: HostFilterPolicy

   .. we document these methods manually so we can specify a param to predicate

   .. automethod:: predicate(host)
   .. automethod:: distance
   .. automethod:: make_query_plan

.. autoclass:: DefaultLoadBalancingPolicy
   :members:

.. autoclass:: DSELoadBalancingPolicy
   :members:

Translating Server Node Addresses
---------------------------------

.. autoclass:: AddressTranslator
   :members:

.. autoclass:: IdentityTranslator
   :members:

.. autoclass:: EC2MultiRegionTranslator
   :members:

Marking Hosts Up or Down
------------------------

.. autoclass:: ConvictionPolicy
   :members:

.. autoclass:: SimpleConvictionPolicy
   :members:

Reconnecting to Dead Hosts
--------------------------

.. autoclass:: ReconnectionPolicy
   :members:

.. autoclass:: ConstantReconnectionPolicy
   :members:

.. autoclass:: ExponentialReconnectionPolicy
   :members:

Retrying Failed Operations
--------------------------

Application retries honor the policy's destination: ``RETRY`` uses the same
coordinator and ``RETRY_NEXT_HOST`` uses another eligible coordinator. If that
destination is unavailable, the original failure is raised unless the client
deadline has expired, in which case ``OperationTimedOut`` takes precedence.
These rules also apply when optional control-connection query fallback is enabled.
An uncertain
application send failure consults the retry policy after transport cleanup;
``FallthroughRetryPolicy`` prevents another policy-driven application send.
Speculative attempts retain their separate execution rules.

.. autoclass:: WriteType
   :members:

.. autoclass:: RetryPolicy
   :members:

.. autoclass:: FallthroughRetryPolicy
   :members:

.. autoclass:: DowngradingConsistencyRetryPolicy
   :members:

Retrying Idempotent Operations
------------------------------

.. autoclass:: SpeculativeExecutionPolicy
   :members:

.. autoclass:: ConstantSpeculativeExecutionPolicy
   :members:
