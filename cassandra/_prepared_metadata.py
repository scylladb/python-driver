# Copyright DataStax, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Immutable decoder snapshots and provenance for prepared metadata."""

import logging
from threading import Lock
from types import MappingProxyType

from cassandra.protocol import (
    ResultMessage,
    _class_attribute,
    _prepared_metadata_cache_config,
    _prepared_metadata_result_attributes,
)


# Preserve the logger used for prepared-decoder fallback diagnostics.
log = logging.getLogger('cassandra.cluster')

_PREPARED_METADATA_HANDLER_SNAPSHOTS = {}
_PREPARED_METADATA_HANDLER_SNAPSHOT_LOCK = Lock()


def protocol_handler_snapshot(protocol_handler):
    """
    Freeze the result-decoder configuration of a driver-owned handler.

    The public handler maps are intentionally customizable. Only the
    canonical driver-owned configuration is eligible: application
    changes fall back to full metadata because arbitrary decoder class
    state cannot be frozen safely. An eligible prepared future receives
    a private snapshot so a later source mutation cannot produce a
    hybrid old-metadata/new-decoder request.

    Custom or modified handlers are returned unchanged and remain
    ineligible for skip-metadata.
    """
    try:
        handler_vars = vars(protocol_handler)
    except TypeError:
        log.debug(
            "Cannot inspect protocol handler %r; prepared executions will "
            "request full result metadata.", protocol_handler,
            exc_info=True)
        return protocol_handler

    if handler_vars.get('_prepared_metadata_handler_snapshot'):
        return protocol_handler

    handler_token = handler_vars.get('_prepared_metadata_cache_token')
    if handler_token is None:
        return protocol_handler

    try:
        message_types = dict(protocol_handler.message_types_by_opcode)
        result_message_type = message_types[ResultMessage.opcode]
        if not isinstance(result_message_type, type):
            return protocol_handler
        type_codes = dict(getattr(result_message_type, 'type_codes', {}))
        cqltypes_by_code = dict(
            getattr(result_message_type, '_cqltypes_by_code', {}))
        decode_message = _class_attribute(
            protocol_handler, 'decode_message')
        result_attributes = \
            _prepared_metadata_result_attributes(result_message_type)
        column_encryption_policy = getattr(
            protocol_handler, 'column_encryption_policy', None)
        handler_config = _prepared_metadata_cache_config(
            protocol_handler,
            result_message_type,
            message_types,
            type_codes,
            cqltypes_by_code,
            decode_message,
            result_attributes,
            column_encryption_policy,
        )
        canonical_config = handler_vars.get(
            '_prepared_metadata_cache_config')
        if (canonical_config is None
                or handler_config[:-1] != canonical_config[:-1]
                or column_encryption_policy is not canonical_config[-1]):
            return protocol_handler
        cache_key = (
            protocol_handler,
            handler_token,
            handler_config[:-1],
            id(column_encryption_policy),
        )
        # A malformed in-place customization should fall back to full
        # metadata, not make request construction fail or create an
        # uncached snapshot whose identity never matches the PREPARE
        # provenance.
        hash(cache_key)
    except Exception:
        log.debug(
            "Cannot validate protocol handler %r; prepared executions will "
            "request full result metadata.", protocol_handler,
            exc_info=True)
        return protocol_handler

    with _PREPARED_METADATA_HANDLER_SNAPSHOT_LOCK:
        try:
            cached = _PREPARED_METADATA_HANDLER_SNAPSHOTS.get(
                protocol_handler)
            if cached is not None and cached[0] == cache_key:
                return cached[1]
        except Exception:
            log.debug(
                "Cannot compare the cached protocol handler snapshot for %r; "
                "prepared executions will request full result metadata.",
                protocol_handler, exc_info=True)
            return protocol_handler

        try:
            result_attrs = dict(result_attributes)
            result_attrs.update({
                '__module__': result_message_type.__module__,
                'type_codes': MappingProxyType(type_codes),
                '_cqltypes_by_code': MappingProxyType(cqltypes_by_code),
            })
            result_snapshot = type(
                '_%sPreparedMetadataSnapshot' %
                result_message_type.__name__,
                (result_message_type,),
                result_attrs,
            )

            message_types[ResultMessage.opcode] = result_snapshot
            handler_attrs = {
                '__module__': protocol_handler.__module__,
                'message_types_by_opcode': MappingProxyType(message_types),
                'column_encryption_policy': column_encryption_policy,
                '_prepared_metadata_cache_token': object(),
                '_prepared_metadata_handler_snapshot': True,
            }
            if decode_message is not None:
                handler_attrs['decode_message'] = decode_message
            snapshot = type(
                '%sPreparedMetadataSnapshot' %
                protocol_handler.__name__,
                (protocol_handler,),
                handler_attrs,
            )
        except Exception:
            log.debug(
                "Cannot freeze protocol handler %r; prepared executions will "
                "request full result metadata.", protocol_handler,
                exc_info=True)
            return protocol_handler
        # Keep only the latest source configuration in the shared cache.
        # Futures retain any older snapshot they are already using.
        _PREPARED_METADATA_HANDLER_SNAPSHOTS[protocol_handler] = (
            cache_key, snapshot)
        return snapshot


def decoder_context(protocol_handler, cluster_context):
    """
    Return cache provenance for a frozen driver-owned protocol handler.

    Custom subclasses can mutate ``message_types_by_opcode`` and their
    result type maps in place. Class identity alone therefore cannot
    prove that cached column descriptors are still valid. Eligible
    handlers are copied into a cached snapshot with read-only maps;
    inherited tokens deliberately do not opt a custom subclass into
    skip-metadata.
    """
    # ``None`` marks a client-side metadata transition. Responses
    # decoded while the transition is in progress must never become
    # eligible for skip-metadata, even if another execute observes the
    # same marker.
    if cluster_context is None:
        return None

    try:
        handler_vars = vars(protocol_handler)
    except TypeError:
        return None
    if not handler_vars.get('_prepared_metadata_handler_snapshot'):
        return None

    return (
        handler_vars['_prepared_metadata_cache_token'],
        protocol_handler,
        cluster_context,
    )


def session_handler(session, protocol_handler):
    """
    Return the Session's immutable snapshot for ``protocol_handler``.

    A Session keeps using the snapshot even if the source class is later
    mutated in place. Assigning a different client protocol handler
    invalidates the cache by identity and builds (or rejects) a new
    snapshot.
    """
    cached = vars(session).get('_prepared_metadata_handler_cache')
    if (cached is not None
            and (cached[0] is protocol_handler
                 or cached[1] is protocol_handler)):
        return cached[1]

    snapshot = protocol_handler_snapshot(protocol_handler)
    session._prepared_metadata_handler_cache = (protocol_handler, snapshot)
    return snapshot
