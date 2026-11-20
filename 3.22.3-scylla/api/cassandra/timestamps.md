# `cassandra.timestamps` - Timestamp Generation

### *class* cassandra.timestamps.MonotonicTimestampGenerator(warn_on_drift=True, warning_threshold=0, warning_interval=0)

An object that, when called, returns `int(time.time() * 1e6)` when
possible, but, if the value returned by `time.time` doesn’t increase,
drifts into the future and logs warnings.
Exposed configuration attributes can be configured with arguments to
`__init__` or by changing attributes on an initialized object.

#### Versionadded
Added in version 3.8.0.

#### warn_on_drift *= True*

If true, log warnings when timestamps drift into the future as allowed by
[`warning_threshold`](#cassandra.timestamps.MonotonicTimestampGenerator.warning_threshold) and [`warning_interval`](#cassandra.timestamps.MonotonicTimestampGenerator.warning_interval).

#### warning_threshold *= 1*

This object will only issue warnings when the returned timestamp drifts
more than `warning_threshold` seconds into the future.
Defaults to 1 second.

#### warning_interval *= 1*

This object will only issue warnings every `warning_interval` seconds.
Defaults to 1 second.

#### \_next_timestamp(now, last)

Returns the timestamp that should be used if `now` is the current
time and `last` is the last timestamp returned by this object.
Intended for internal and testing use only; to generate timestamps,
call an instantiated `MonotonicTimestampGenerator` object.

* **Parameters:**
  * **now** (*int*) – an integer to be used as the current time, typically
    representing the current time in microseconds since the UNIX epoch
  * **last** (*int*) – an integer representing the last timestamp returned by
    this object
