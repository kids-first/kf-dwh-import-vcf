package org.kidsfirstdrc.dwh.jobs

import java.sql.Timestamp
import java.time.LocalDateTime

/**
 * Metadata to store for each table
 * @param table_name the full name of the concrete table. ie variant.occurrences for example
 * @param location absolute path where the files are stored
 * @param format format of the data
 * @param last_update the timestamp of the last update
 * @param current_release release number in kidsfirst systems. for instance: RE_01
 * @param origin_release release number in origin systems. for instance gnomadV3.1.1
 */
case class Metadata(table_name: String,
                    location: String,
                    format: String,
                    current_release: String,
                    origin_release: String,
                    last_update: Timestamp = Timestamp.valueOf(LocalDateTime.now()))
