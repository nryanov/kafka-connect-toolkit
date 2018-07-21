package com.github.gr1f0n6x.querydsl.dialects

import com.github.gr1f0n6x.querydsl.*
import com.github.gr1f0n6x.querydsl.Create

abstract class PgDataType : DataType() {
    class BIGINT : DataType() {
        override fun definition(): String = "BIGINT"
    }

    class BIGSERIAL : DataType() {
        override fun definition(): String = "BIGSERIAL"
    }

    class BIT(val size: Int = 1) : DataType() {
        override fun definition(): String = "BIT($size)"
    }

    class VARBIT(val size: Int = 1) : DataType() {
        override fun definition(): String = "BIT VARYING($size)"
    }

    class BOOLEAN : DataType() {
        override fun definition(): String = "BOOLEAN"
    }

    class BOX : DataType() {
        override fun definition(): String = "BOX"
    }

    class BYTEA : DataType() {
        override fun definition(): String = "BYTEA"
    }

    class CHAR(val size: Int = 1) : DataType() {
        override fun definition(): String = "CHAR($size)"
    }

    class VARCHAR(val size: Int = 1) : DataType() {
        override fun definition(): String = "VARCHAR($size)"
    }

    class CIDR : DataType() {
        override fun definition(): String = "CIDR"
    }

    class CIRCLE : DataType() {
        override fun definition(): String = "CIRCLE"
    }

    class DATE : DataType() {
        override fun definition(): String = "DATE"
    }

    class DOUBLE_PRECISION : DataType() {
        override fun definition(): String = "FLOAT8"
    }

    class INET : DataType() {
        override fun definition(): String = "INET"
    }

    class INT : DataType() {
        override fun definition(): String = "INT"
    }

    class JSON : DataType() {
        override fun definition(): String = "JSON"
    }

    class JSONB : DataType() {
        override fun definition(): String = "JSONB"
    }

    class LINE : DataType() {
        override fun definition(): String = "LINE"
    }

    class LSEG : DataType() {
        override fun definition(): String = "LSEG"
    }

    class MACADDR : DataType() {
        override fun definition(): String = "MACADDR"
    }

    class MACADDR8 : DataType() {
        override fun definition(): String = "MACADDR8"
    }

    class MONEY : DataType() {
        override fun definition(): String = "MONEY"
    }

    class NUMERIC(val p: Int = 1, val s: Int = 0) : DataType() {
        override fun definition(): String = "NUMERIC($p, $s)"
    }

    class PATH : DataType() {
        override fun definition(): String = "PATH"
    }

    class PG_LSN : DataType() {
        override fun definition(): String = "PG_LSN"
    }

    class POINT : DataType() {
        override fun definition(): String = "POINT"
    }

    class POLYGON : DataType() {
        override fun definition(): String = "POLYGON"
    }

    class REAL : DataType() {
        override fun definition(): String = "REAL"
    }

    class SMALLINT : DataType() {
        override fun definition(): String = "INT2"
    }

    class SMALLSERIAL : DataType() {
        override fun definition(): String = "SERIAL2"
    }

    class SERIAL : DataType() {
        override fun definition(): String = "SERIAL4"
    }

    class TEXT : DataType() {
        override fun definition(): String = "TEXT"
    }

    class TIME : DataType() {
        override fun definition(): String = "TIME"
    }

    class TIME_WITH_TIMEZONE : DataType() {
        override fun definition(): String = "TIMEZ"
    }

    class TIMESTAMP : DataType() {
        override fun definition(): String = "TIMESTAMP"
    }

    class TIMESTAMP_WITH_TIME_ZONE : DataType() {
        override fun definition(): String = "TIMESTAMPZ"
    }

    class TSQUERY : DataType() {
        override fun definition(): String = "TSQUERY"
    }

    class TSVECTOR : DataType() {
        override fun definition(): String = "TSVECTOR"
    }

    class TXID_SNAPSHOT : DataType() {
        override fun definition(): String = "TXID_SNAPSHOT"
    }

    class UUID : DataType() {
        override fun definition(): String = "UUID"
    }

    class XML : DataType() {
        override fun definition(): String = "XML"
    }
}

enum class OnCommitType(val value: String) {
    PRESERVE_ROWS("PRESERVE ROWS"), DELETE_ROWS("DELETE ROWS"), DROP("DROP")
}

enum class PartitionType {
    RANGE, LIST
}

enum class ActionType {
    RESTRICT, NO_ACTION, CASCADE, SET_NULL, SET_DEFAULT
}

enum class Parameters(val value: String) {
    FILLFACTOR("fillfactor"), PARALLEL_WORKERS("parallel_workers"),
    AUTOVACUUM_ENABLED("autovacuum_enabled"), AUTOVACUUM_VACUUM_THRESHOLD("autovacuum_vacuum_threshold"),
    AUTOVACUUM_VACUUM_SCALE_FACTOR("autovacuum_vacuum_scale_factor"), AUTOVACUUM_ANALAYZE_THRESHOLD("autovacuum_analyze_threshold"),
    AUTOVACUUM_ANALYZE_SCLAE_FACTOR("autovacuum_analyze_scale_factor"), AUTOVACUUM_VACUUM_COST_DELAY("autovacuum_vacuum_cost_delay"), AUTOVACUUM_VACUUM_COST_LIMIT("autovacuum_vacuum_cost_limit"),
    AUTOVACUUM_FREEZE_MIN_AGE("autovacuum_freeze_min_age"), AUTOVACUUM_FREEZE_MAX_AGE("autovacuum_freeze_max_age"), AUTOVACUUM_FREEZE_TABLE_AGE("autovacuum_freeze_table_age"),
    AUTOVACUUM_MULTIXACT_FREEZE_MIN_AGE("autovacuum_multixact_freeze_min_age"), AUTOVACUUM_MULTIXACT_FREEZE_MAX_AGE("autovacuum_multixact_freeze_max_age"),
    AUTOVACUUM_MULTIXACT_FREEZE_TABLE_AGE("autovacuum_multixact_freeze_table_age"), LOG_AUTOVACUUM_MIN_DURATION("log_autovacuum_min_duration"), USER_CATALOG_TABLE("user_catalog_table")
}

class PgForeignKey(private val columns: Array<Column>, private val reference: Table, private val columnsReference: Array<Column>) : ForeignKey(columns, reference, columnsReference) {
    private var onDelete: ActionType = ActionType.NO_ACTION
    private var onUpdate: ActionType = ActionType.NO_ACTION

    fun onDelete(action: ActionType): PgForeignKey = this.apply { this.onDelete = action }

    fun onUpdate(action: ActionType): PgForeignKey = this.apply { this.onUpdate = action }

    override fun toSQL(): String {
        return super.toSQL()
    }
}

enum class SelectType(val value: String) {
    UPDATE("UPDATE"), NO_KEY_UPDATE("NO KEY UPDATE"), SHARE("SHARE"), KEY_SHARE("KEY SHARE")
}

object PgQueryBuilder {
    fun select(columns: Array<Value>): PgSelect = PgSelect(columns)

    fun update(table: Table): Update = TODO()

    fun insert(table: Table): PgInsert = PgInsert(table)

    fun delete(table: Table): Delete = TODO()

    fun create(): PgCreate = PgCreate()

    fun truncate(table: Table): Truncate = TODO()

    fun drop(): Drop = TODO()

    fun alter(): Alter = TODO()
}

class PgSelect(private val columns: Array<Value> = arrayOf(ALL())) : Select(columns) {
    private var selectType: SelectType? = null

    override fun distinct(): PgSelect {
        return super.distinct() as PgSelect
    }

    override fun from(source: Source): From<PgSelect> {
        return super.from(source) as From<PgSelect>
    }

    override fun groupBy(vararg columns: Column): PgSelect {
        return super.groupBy(*columns) as PgSelect
    }

    override fun limit(limit: Int): PgSelect {
        return super.limit(limit) as PgSelect
    }

    override fun offset(number: Int): PgSelect {
        return super.offset(number) as PgSelect
    }

    override fun orderBy(vararg order: Order): PgSelect {
        return super.orderBy(*order) as PgSelect
    }

    override fun having(condition: Condition): PgSelect {
        return super.having(condition) as PgSelect
    }

    override fun aS(alias: String): PgSelect {
        return super.aS(alias) as PgSelect
    }

    fun forOption(type: SelectType): PgSelect {
        if (this.selectType != null) {
            throw RuntimeException("For clause in select is already defined")
        }

        this.selectType = type

        return this
    }

    override fun toSQL(): String {
        return super.toSQL() + "" +
                "${if (selectType != null) "\nFOR ${selectType!!.value}" else ""}"
    }
}

class OnConflict(): Clause {
    private var doNothing: Boolean = false
    private var where: Condition? = null
    private var constraint: String? = null
    private var columns: Array<Column>? = null
    private val updates: MutableMap<Column, BaseValue> = mutableMapOf()

    fun onConstraint(name: String): OnConflict {
        if (this.columns != null) {
            throw RuntimeException("Columns is already defined. Using constraint name is impossible")
        }

        if (this.constraint != null) {
            throw RuntimeException("Constraint name is already defined")
        }

        this.constraint = name

        return this
    }

    fun onColumns(columns: Array<Column>): OnConflict {
        if (this.columns != null) {
            throw RuntimeException("Columns is already defined. Using constraint name is impossible")
        }

        if (this.constraint != null) {
            throw RuntimeException("Constraint name is already defined")
        }

        this.columns = columns

        return this
    }

    fun set(column: Column, newValue: Column): OnConflict {
        if (this.doNothing) {
            throw RuntimeException("Attempt to override do nothing")
        }

        updates.put(column, newValue)

        return this
    }

    fun set(column: Column, newValue: Literal): OnConflict {
        if (this.doNothing) {
            throw RuntimeException("Attempt to override do nothing")
        }

        updates.put(column, newValue)

        return this
    }

    fun where(condition: Condition): OnConflict {
        if (this.where != null) {
            throw RuntimeException("Where clause is already defined")
        }

        where = condition

        return this
    }

    fun doNothing(): OnConflict = this.apply { this.doNothing = true }

    override fun toSQL(): String = "ON CONFLICT ${if (constraint != null) "ON $constraint" else if (columns != null) "(${columns!!.map { x -> x.forAction() }.joinToString(separator = ", ")})" else ""}" +
            "${if (where != null) "\nWHERE ${where!!.toSQL()}" else ""}" +
            "\nDO ${if (doNothing) "NOTHING" else "UPDATE"}" +
            "${if (updates != null) "\nSET ${updates.map { x -> "${x.key} = ${x.value}" }.joinToString(separator = ", ")}" else ""}"
}

class PgInsert(table: Table) : Insert(table) {
    private var useDefaultValues: Boolean = false
    private var returning: Array<Column>? = null
    private var returningAll: Boolean = false
    private var onConflict: OnConflict? = null

    override fun columns(vararg columns: Column): PgInsert {
        return super.columns(*columns) as PgInsert
    }

    fun defaultValues(): PgInsert = this.apply { useDefaultValues = true }

    fun onConflict(onConflict: OnConflict): PgInsert {
        if (this.onConflict != null) {
            throw RuntimeException("OnConflict block is already defined")
        }

        this.onConflict = onConflict

        return this
    }

    fun returning(columns: Array<Column>): PgInsert {
        if (this.returning != null) {
            throw RuntimeException("Returning block is already defined")
        }

        if (this.returningAll) {
            throw RuntimeException("Returning all is already used")
        }

        this.returning = columns

        return this
    }

    fun returningAll(): PgInsert = this.apply { this.returningAll = true }

    override fun values(vararg values: Literal): PgInsert {
        if (useDefaultValues) {
            throw RuntimeException("Attempt to override default values")
        }

        return super.values(*values) as PgInsert
    }

    override fun toSQL(): String = "INSERT INTO ${table.forAction()}" +
            "${if (columnLst!!.isNotEmpty()) "(${columnLst!!.map { x -> x.forAction() }.joinToString(separator = ", ")})" else ""} " +
            "${if (useDefaultValues) "DEFAULT VALUES" else "VALUES(${values.joinToString(separator = ", ")})"}" +
            "${if (onConflict != null) "\n${onConflict!!.toSQL()}" else ""}" +
            "${if (returning != null) "\nRETURNING (${returning!!.map { x -> x.forAction() }.joinToString(separator = ", ")})" else if (returningAll) "\nRETURNING *" else ""}"
}

class PgCreate: Create() {
    override fun table(schema: String?, name: String, caseSensitive: Boolean): CreatePgTable = CreatePgTable(schema, name, caseSensitive)

    inner class CreatePgTable(schema: String? = null, name: String, caseSensitive: Boolean = false) : Create.CreateTable(schema, name, caseSensitive) {
        private var isTemp: Boolean = false
        private var ifNotExists: Boolean = false
        private var unlogged: Boolean = false
        private var inherits: Array<Table>? = null
        private var like: Table? = null
        private var tablespace: String? = null
        private var onCommitType: OnCommitType? = null
        private var with: Map<Parameters, Any>? = null

        private var partitionType: PartitionType? = null

        fun temporary(): CreatePgTable = this.apply { isTemp = true }

        fun ifNotExists(): CreatePgTable = this.apply { ifNotExists = true }

        fun unlogged(): CreatePgTable = this.apply { unlogged = true }

        fun tablespace(tablespace: String): CreatePgTable {
            if (this.tablespace != null) {
                throw RuntimeException("Tablespace block is already defined")
            }

            this.tablespace = tablespace

            return this
        }

        override fun addColumn(name: String, type: DataType): CreatePgTable {
            return super.addColumn(name, type) as CreatePgTable
        }

        override fun primaryKey(primaryKey: PrimaryKey): CreatePgTable {
            return super.primaryKey(primaryKey) as CreatePgTable
        }

        override fun foreignKeys(foreignKeys: Array<ForeignKey>): CreatePgTable {
            return super.foreignKeys(foreignKeys) as CreatePgTable
        }

        override fun checks(checks: Array<Check>): CreatePgTable {
            return super.checks(checks) as CreatePgTable
        }

        fun onCommit(onCommitType: OnCommitType): CreatePgTable {
            if (this.onCommitType != null) {
                throw RuntimeException("On commit block is already defined")
            }

            this.onCommitType = onCommitType

            return this
        }

        fun with(parameters: Map<Parameters, Any>): CreatePgTable {
            if (this.with != null) {
                throw RuntimeException("With block is already defined")
            }

            this.with = parameters

            return this
        }

        fun inherits(tables: Array<Table>): CreatePgTable {
            if (this.inherits != null) {
                throw RuntimeException("Inherits block is already defined")
            }

            this.inherits = tables

            return this
        }

        fun like(table: Table): CreatePgTable {
            if (this.like != null) {
                throw RuntimeException("Like block is already defined")
            }

            this.like = table

            return this
        }

        override fun toSQL(): String {
            if (columns == null || columns.size == 0) {
                throw RuntimeException("Please, specify at least one column for table")
            }

            return "CREATE TABLE${if (isTemp) " TEMP" else ""}${if (unlogged) " UNLOGGED" else ""}${if (ifNotExists) " IF NOT EXISTS" else ""}" +
                    " ${table.forAction()} (\n" +
                    "${columns.map { x -> "\t${x.first} ${x.second.toSQL()}" }.joinToString(separator = ", \n")}" +
                    "${if (primaryKey != null) ",\n${primaryKey!!.toSQL()}" else ""}" +
                    "${if (foreignKeys != null) ",\n${foreignKeys!!.map { x -> x.toSQL() }.joinToString(separator = ",\n")}" else ""}" +
                    "${if (checks != null) ",\n${checks!!.map { x -> x.toSQL() }.joinToString(separator = ",\n")}" else ""}" +
                    "${if (like != null) ",\nLIKE ${like!!.forAction()}" else ""}" +
                    ")" +
                    "${if (inherits != null) "\nINHERITS (${inherits!!.map { x -> x.forAction() }.joinToString(separator = ", ")})" else ""}" +
                    "${if (with != null) "\nWITH (${with!!.map { x -> x.key.value + "=" + x.value }.joinToString(separator = ", ")})" else ""}" +
                    "${if (onCommitType != null) "\nON COMMIT ${onCommitType!!.value}" else ""}" +
                    "${if (tablespace != null) "\nTABLESPACE $tablespace" else ""}"
        }
    }
}