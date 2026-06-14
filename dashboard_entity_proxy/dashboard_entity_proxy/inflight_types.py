# Copyright (c) 2026 Kenneth Baker <bakerkj@umich.edu>
# All rights reserved.

"""Sealed union of inflight dispatch-table entry kinds.

Every entry the proxy puts into ``Session._inflight_table`` is one of
these dataclasses. ``_dispatch_inflight`` routes inbound HA frames by
``match``-ing on the entry's type, so the union is exhaustive and mypy
verifies the dispatch.

Two families:

* **Client-originated**: represent a translated client request whose
  HA response needs to be rewritten back to the client's id. These
  carry the original client id in ``client_id``.

* **Proxy-originated**: long-lived subscriptions (``Mirror``,
  ``EntityUpdates``, ``DeviceUpdates``, ``LovelaceUpdates``) plus
  one-shot requests (``EntityList``, ``DeviceList``, ``EntityGet``,
  ``ConfigFetch``, ``EnergyPrefs``). These don't have a client id;
  their results are consumed internally by the session.
"""

from __future__ import annotations

from dataclasses import dataclass


# ---- Client-originated -----------------------------------------------------


@dataclass(frozen=True, slots=True)
class ClientReq:
    """A translated client request whose response (and any subsequent
    subscription events) must be rewritten back to ``client_id`` and
    forwarded. The entry persists for the life of the connection (or
    until the client explicitly unsubscribes): the proxy cannot know
    at request time whether a command opens a streaming subscription
    (``render_template``, ``camera/stream``, integration-specific
    ``*/subscribe_*``, etc.), so any popping risks ``Received event for
    unknown subscription`` errors on the browser side.
    """

    client_id: int


@dataclass(frozen=True, slots=True)
class ClientConfig:
    """A translated client ``lovelace/config`` request. Response is
    forwarded to the client AND fed into scope resolution for ``url``.
    """

    client_id: int
    url: str


@dataclass(frozen=True, slots=True)
class ClientUnsubscribe:
    """A translated client ``unsubscribe_events`` whose ``subscription``
    field referred to ``ha_sub_id``. On success ack, drop ``ha_sub_id``
    from inflight / known-subscriptions / pop_at; on failure log + drop
    anyway (the client believes the subscription is gone, so the proxy
    cannot leave the entry routing future events).
    """

    client_id: int
    ha_sub_id: int


# ---- Proxy-originated, long-lived subscriptions ----------------------------


@dataclass(frozen=True, slots=True)
class Mirror:
    """The state-mirror ``subscribe_entities`` subscription (long-lived)."""


@dataclass(frozen=True, slots=True)
class EntityUpdates:
    """The entity-registry update subscription (long-lived)."""


@dataclass(frozen=True, slots=True)
class DeviceUpdates:
    """The device-registry update subscription (long-lived)."""


@dataclass(frozen=True, slots=True)
class LovelaceUpdates:
    """Subscription to HA's ``lovelace_updated`` event, used to invalidate
    ``DashboardCache`` entries when a dashboard's YAML / storage config
    is edited.
    """


# ---- Proxy-originated, one-shot --------------------------------------------


@dataclass(frozen=True, slots=True)
class EntityList:
    """A ``config/entity_registry/list`` response."""


@dataclass(frozen=True, slots=True)
class DeviceList:
    """A ``config/device_registry/list`` response."""


@dataclass(frozen=True, slots=True)
class EntityGet:
    """An incremental-mode per-entity fetch triggered by an
    ``entity_registry_updated`` create/update event.
    """

    entity_id: str


@dataclass(frozen=True, slots=True)
class ConfigFetch:
    """A proxy-injected ``lovelace/config`` fetch for ``url_path``."""

    url_path: str


@dataclass(frozen=True, slots=True)
class EnergyPrefs:
    """A proxy-injected ``energy/get_prefs`` request."""


InflightEntry = (
    ClientReq
    | ClientConfig
    | ClientUnsubscribe
    | Mirror
    | EntityUpdates
    | DeviceUpdates
    | LovelaceUpdates
    | EntityList
    | DeviceList
    | EntityGet
    | ConfigFetch
    | EnergyPrefs
)
