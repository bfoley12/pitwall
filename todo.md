- ExtrapolatedClock (seems useless)
- SessionStatus (seems useless)
- TopThree (redundant position info)
- Heartbeat (essentially a ping. Only thing to maybe do is translate race time to utc, but that can be done with other info that we have)
- TlaRcm (covered in RaceControlMessages)
- OvertakeSeries (we have this in DriverRaceInfo)

Add documentation to literally everything... So lazy...


*** Can use weather data from teams (Williams) for weekend forcast ahead of time - would need to go in a higher level API
- could be fun to make a dashboard for anticipated weather, schedule etc. a la F1 Unchained 2026 Japanese GP prediction