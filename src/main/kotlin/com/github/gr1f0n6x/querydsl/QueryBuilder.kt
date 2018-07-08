package com.github.gr1f0n6x.querydsl

import java.util.Date

enum class ConditionType(val symbol: String) {
    EQ("="), NEQ("!="), GE(">="), LE("<="), GT(">"), LT("<"), LIKE("LIKE"), IN("IN"),
    NOT("NOT"), OR("OR"), AND("AND"), RAW(""), IS("IS"), IS_NOT("IS NOT"), BETWEEN("BETWEEN")
}

enum class OrderType(val symbol: String) {
    ASC("ASC"), DESC("DESC")
}

enum class StatementType(val type: QueryType) {
    INSERT(QueryType.DML), UPDATE(QueryType.DML), DELETE(QueryType.DML), SELECT(QueryType.DML),
    CREATE(QueryType.DDL), ALTER(QueryType.DDL), TRUNCATE(QueryType.DDL), DROP(QueryType.DDL),
    GRANT(QueryType.DDL)
}

enum class JoinType(val type: String) {
    INNER("INNER JOIN"), LEFT("LEFT JOIN"), RIGHT("RIGHT JOIN"), FULL("FULL JOIN")
}

enum class QueryType {
    DDL, DML
}

enum class AggregationType(val symbol: String) {
    COUNT("COUNT"), MAX("MAX"), MIN("MIN"), AVG("AVG")
}

interface Expression {
    fun toSQL(): String
}

interface Alias {
    infix fun aS(alias: String): Alias
}

interface Source : Expression, Alias

interface Value : Expression, Alias

interface Statement : Expression

interface Clause : Statement

interface ClauseBuilder<out T : Clause> {
    fun build(): T
}

interface Constraint : Expression

abstract class BaseValue : Value {
    protected var alias: String? = null

    override infix fun aS(alias: String): BaseValue = this.apply { this.alias = alias }

    fun between(lower: BaseValue, upper: BaseValue): Condition = TernaryConditionBuilder(ConditionType.BETWEEN).apply { left = lower; mid = this@BaseValue; right = upper }.build()

    infix fun eq(value: Value): Condition = BinaryConditionBuilder(ConditionType.EQ).apply { left = this@BaseValue; right = value }.build()

    infix fun neq(value: Value): Condition = BinaryConditionBuilder(ConditionType.NEQ).apply { left = this@BaseValue; right = value }.build()

    infix fun ge(value: Value): Condition = BinaryConditionBuilder(ConditionType.GE).apply { left = this@BaseValue; right = value }.build()

    infix fun le(value: Value): Condition = BinaryConditionBuilder(ConditionType.LE).apply { left = this@BaseValue; right = value }.build()

    infix fun gt(value: Value): Condition = BinaryConditionBuilder(ConditionType.GT).apply { left = this@BaseValue; right = value }.build()

    infix fun lt(value: Value): Condition = BinaryConditionBuilder(ConditionType.LT).apply { left = this@BaseValue; right = value }.build()

    infix fun iS(value: Value): Condition = BinaryConditionBuilder(ConditionType.IS).apply { left = this@BaseValue; right = value }.build()

    infix fun iSNot(value: Value): Condition = BinaryConditionBuilder(ConditionType.IS_NOT).apply { left = this@BaseValue; right = value }.build()

    infix fun like(value: Value): Condition = BinaryConditionBuilder(ConditionType.LIKE).apply { left = this@BaseValue; right = value }.build()

    infix fun iN(value: Value): Condition = BinaryConditionBuilder(ConditionType.IN).apply { left = this@BaseValue; right = value }.build()
}

// TODO: pattern matching instead of concrete data type classes
abstract class Literal(protected val literal: Any? = NULL()) : BaseValue() {
    override fun toString(): String = this.toSQL()

    protected fun aliasWrapper(value: Any): String = "$value${if (alias != null) " as $alias" else ""}"

    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (javaClass != other?.javaClass) return false

        other as Literal

        if (literal != other.literal) return false

        return true
    }

    override fun hashCode(): Int {
        return if (literal != null) {
            literal.hashCode()
        } else {
            0
        }
    }

    override fun toSQL(): String = "${escapedValue(literal)}${if (alias != null) " as $alias" else ""}"
}


abstract class DataType : Expression {
    private var notNull: Boolean = false
    private var unique: Boolean = false
    private var default: Literal? = null

    protected abstract fun definition(): String

    override fun toSQL(): String {
        return "${this.definition()}${if (notNull) " NOT NULL" else ""}${if (unique) " UNIQUE" else ""}${if (default != null) " DEFAULT ${default!!.toSQL()}" else ""}"
    }

    fun notNull(): DataType = this.apply { notNull = true }

    fun unique(): DataType = this.apply { unique = true }

    fun default(literal: Literal): DataType = this.apply { default = literal }

    class BIT(val size: Int = 1) : DataType() { // 0, 1, NULL
        override fun definition(): String = "BIT($size)"
    }

    class BIT_VARYING(val size: Int = 1) : DataType() { // 0, 1, NULL
        override fun definition(): String = "BIT VARYING($size)"
    }

    class DATE : DataType() {
        override fun definition(): String = "DATE"
    }

    class TIME : DataType() {
        override fun definition(): String = "TIME"
    }

    class TIMESTAMP : DataType() {
        override fun definition(): String = "TIMESTAMP"
    }

    class DECIMAL(val precision: Int = 18, val scale: Int? = null) : DataType() {
        override fun definition(): String = "DECIMAL($precision${if (scale != null) ", $scale" else ""})"
    }

    class REAL : DataType() {
        override fun definition(): String = "REAL"
    }

    class FLOAT(val bitSize: Int = 53) : DataType() {
        override fun definition(): String = "FLOAT($bitSize)"
    }

    class SMALLINT : DataType() {
        override fun definition(): String = "SMALLINT"
    }

    class INTEGER : DataType() {
        override fun definition(): String = "INTEGER"
    }

    class CHAR(val size: Int = 1) : DataType() {
        override fun definition(): String = "CHAR($size)"
    }

    class NCHAR(val size: Int = 1) : DataType() {
        override fun definition(): String = "NCHAR($size)"
    }

    class VARCHAR(val size: Int = 1) : DataType() {
        override fun definition(): String = "VARCHAR($size)"
    }

    class NVARCHAR(val size: Int = 1) : DataType() {
        override fun definition(): String = "NVARCHAR($size)"
    }
}

abstract class ForwardStatement<out T : Statement>(private val statement: T) : Statement {
    // TODO: remove and()
    fun and(): T = statement
}

abstract class Condition(protected val conditionType: ConditionType) : Clause {
    infix fun or(condition: Condition): Condition = BinaryWrapperConditionBuilder(ConditionType.OR).apply { first = this@Condition; second = condition }.build()

    infix fun and(condition: Condition): Condition = BinaryWrapperConditionBuilder(ConditionType.AND).apply { first = this@Condition; second = condition }.build()

    operator fun not(): Condition = UnaryWrapperConditionBuilder(ConditionType.NOT).apply { condition = this@Condition }.build()
}

data class Table(val schema: String? = null, val name: String, val caseSensitive: Boolean = false) : Source, Value {
    private var alias: String? = null

    override fun toString(): String = this.toSQL()

    internal fun forAction(): String {
        return if (caseSensitive) {
            "${if (schema != null) "\"$schema\"." else ""}\"$name\""
        } else {
            "${if (schema != null) "$schema." else ""}$name"
        }
    }

    override infix fun aS(alias: String): Table = this.apply { this.alias = alias }

    override fun toSQL(): String {
        return if (caseSensitive) {
            "${if (schema != null) "\"$schema\"." else ""}\"$name\"${if (alias != null) " $alias" else ""}"
        } else {
            "${if (schema != null) "$schema." else ""}$name${if (alias != null) " $alias" else ""}"
        }
    }
}

data class Index(val schema: String? = null, val name: String, val caseSensitive: Boolean = false) : Expression {
    override fun toString(): String = this.toSQL()

    internal fun forAction(): String = toString()

    override fun toSQL(): String {
        return if (caseSensitive) {
            "${if (schema != null) "\"$schema\"." else ""}\"$name\""
        } else {
            "${if (schema != null) "$schema." else ""}$name"
        }
    }
}

data class Column(val name: String, val tableAlias: String? = null, val caseSensitive: Boolean = false) : BaseValue() {
    override fun toString(): String = this.toSQL()

    internal fun forAction(): String {
        return if (caseSensitive) {
            "\"$name\""
        } else {
            name
        }
    }

    override fun toSQL(): String {
        return if (caseSensitive) {
            "${if (tableAlias != null) "$tableAlias." else ""}\"$name\"${if (alias != null) " as $alias" else ""}"
        } else {
            "${if (tableAlias != null) "$tableAlias." else ""}$name${if (alias != null) " as $alias" else ""}"
        }
    }
}

class NULL : Literal("NULL")

class ALL : Literal("*")

class BIT(value: Int? = null) : Literal(value) {
    override fun toString(): String {
        return when {
            literal == null -> aliasWrapper(NULL().toString())
            literal == 0 -> aliasWrapper("0")
            literal != 0 -> aliasWrapper("1")
            else -> aliasWrapper(NULL().toString())
        }
    }
}

class VARBIT(vararg values: Int) : Literal(values) {
    override fun toString(): String {
        return when {
            literal == null -> aliasWrapper(NULL().toString())
            literal == 0 -> aliasWrapper("0")
            literal != 0 -> aliasWrapper("1")
            else -> aliasWrapper(NULL().toString())
        }
    }
}

class DATE(value: Date) : Literal(value)

class TIME(value: Date) : Literal(value)

class TIMESTAMP(value: Date) : Literal(value)

class DECIMAL(value: Long) : Literal(value)

class REAL(value: Float) : Literal(value)

class FLOAT(value: Double) : Literal(value)

class SMALLINT(value: Short) : Literal(value)

class INTEGER(value: Int) : Literal(value)

class CHAR(value: String = "") : Literal(value)

class NCHAR(value: String = "") : Literal(value)

class VARCHAR(value: String = "") : Literal(value)

class NVARCHAR(value: String = "") : Literal(value)

class PrimaryKey(private val columns: Array<Column>) : Constraint {
    override fun toSQL(): String = "PRIMARY KEY (${columns.map { x -> x.forAction() }.joinToString(separator = ", ")})"
}

class ForeignKey(private val columns: Array<Column>, private val reference: Table, private val columnsReference: Array<Column>) : Constraint {
    override fun toSQL(): String = "FOREIGN KEY (${columns.map { x -> x.forAction() }.joinToString(separator = ", ")}) REFERENCES ${reference.forAction()}(${columnsReference.map { x -> x.forAction() }.joinToString(separator = ", ")})"
}

class Check(private val condition: Condition) : Constraint {
    override fun toSQL(): String = "CHECK (${condition.toSQL()})"
}

class RawCondition(private val clause: String) : Condition(ConditionType.RAW) {
    override fun toSQL(): String = clause
}

class UnaryWrapperConditionBuilder(private val conditionType: ConditionType) : ClauseBuilder<Condition> {
    lateinit var condition: Condition

    override fun build(): Condition = UnaryWrapperCondition(conditionType, condition)
}

class BinaryWrapperConditionBuilder(private val conditionType: ConditionType) : ClauseBuilder<Condition> {
    lateinit var first: Condition
    lateinit var second: Condition

    override fun build(): Condition = BinaryWrapperCondition(conditionType, first, second)
}

class UnaryConditionBuilder(private val conditionType: ConditionType) : ClauseBuilder<Condition> {
    lateinit var column: Value

    override fun build(): Condition = UnaryCondition(conditionType, column)
}

class BinaryConditionBuilder(private val conditionType: ConditionType) : ClauseBuilder<Condition> {
    lateinit var left: Value
    lateinit var right: Value

    override fun build(): Condition = BinaryCondition(conditionType, left, right)
}

class TernaryConditionBuilder(private val conditionType: ConditionType) : ClauseBuilder<Condition> {
    lateinit var left: Value
    lateinit var right: Value
    lateinit var mid: Value

    override fun build(): Condition = TernaryCondition(conditionType, left, right, mid)
}

class UnaryWrapperCondition(conditionType: ConditionType, private val condition: Condition) : Condition(conditionType) {
    override fun toSQL(): String = "${conditionType.symbol} ${condition.toSQL()}"
}

class BinaryWrapperCondition(conditionType: ConditionType, private val first: Condition, private val second: Condition) : Condition(conditionType) {
    override fun toSQL(): String = "(${first.toSQL()} ${conditionType.symbol} ${second.toSQL()})"
}

class UnaryCondition(conditionType: ConditionType, private val column: Value) : Condition(conditionType) {
    override fun toSQL(): String = "$column ${conditionType.symbol}"
}

class BinaryCondition(conditionType: ConditionType, private val left: Value, private val right: Value) : Condition(conditionType) {
    override fun toSQL(): String = "$left ${conditionType.symbol} $right"
}

class TernaryCondition(conditionType: ConditionType, private val left: Value, private val right: Value, private val mid: Value) : Condition(conditionType) {
    override fun toSQL(): String = "$mid ${conditionType.symbol} $left AND $right"
}

class From<A : Statement>(private val query: A, private val source: Source) : ForwardStatement<A>(query) {

    private var joins: MutableList<Join<A>> = mutableListOf()
    private var where: Condition? = null

    fun innerJoin(source: Source): Join<A> = createJoin(source, JoinType.INNER)

    fun leftJoin(source: Source): Join<A> = createJoin(source, JoinType.LEFT)

    fun rightJoin(source: Source): Join<A> = createJoin(source, JoinType.RIGHT)

    fun fullJoin(source: Source): Join<A> = createJoin(source, JoinType.FULL)

    private fun createJoin(source: Source, joinType: JoinType): Join<A> {
        val join = Join(this, source, joinType)
        joins.add(join)

        return join
    }

    fun where(condition: Condition): A {
        where = condition

        return query
    }

    fun where(condition: String): A {
        where = RawCondition(condition)

        return query
    }

    override fun toSQL(): String = "FROM $source" +
            "${if (joins.isNotEmpty()) "\n ${joins.map { x -> x.toSQL() }.joinToString(separator = "\n")}" else ""}" +
            "${if (where != null) "\n WHERE ${where!!.toSQL()}" else ""}"
}

class Join<A : Statement>(private val clause: From<A>, private val source: Source, private val joinType: JoinType) : ForwardStatement<From<A>>(clause) {

    private lateinit var condition: Condition

    fun on(condition: Condition): From<A> {
        this.condition = condition

        return clause
    }

    fun on(condition: String): From<A> {
        this.condition = RawCondition(condition)

        return clause
    }

    override fun toSQL(): String = "${joinType.type} $source ON ${condition.toSQL()}"
}

class Aggregation(private val aggregationType: AggregationType, private val column: Any) : BaseValue(), Clause {
    override fun toSQL(): String = "${aggregationType.symbol}($column)"

    override fun toString(): String = this.toSQL()
}

class AggregationBuilder(private val aggregationType: AggregationType) : ClauseBuilder<Aggregation> {
    lateinit var column: Any

    override fun build(): Aggregation = Aggregation(aggregationType, column)
}

class Order(private val orderType: OrderType, private val column: Column) : Clause {
    override fun toSQL(): String = "$column ${orderType.symbol}"
}

class OrderBuilder(private val orderType: OrderType) : ClauseBuilder<Order> {
    lateinit var column: Column

    override fun build(): Order = Order(orderType, column)
}

object QueryBuilder {
    fun select(vararg columns: Value): Select = Select(*columns)

    fun update(table: Table): Update = Update(table)

    fun insert(table: Table): Insert = Insert(table)

    fun delete(table: Table): Delete = Delete(table)

    fun create(): Create = Create()

    fun truncate(table: Table): Truncate = Truncate(table)

    fun drop(): Drop = Drop()

    fun alter(): Alter = Alter()
}

open class Select(private vararg val columns: Value = arrayOf(ALL())) : Statement, Source, Value {
    private var from: From<Select>? = null
    private var limit: Int? = null
    private var groupBy: Array<out Column>? = null
    private var orderBy: Array<out Order>? = null
    private var having: Condition? = null
    private var alias: String? = null

    fun from(source: Source): From<Select> {
        if (this.from != null) {
            throw RuntimeException("From part is already defined")
        }

        from = From(this, source)

        return from as From<Select>
    }

    fun groupBy(vararg columns: Column): Select {
        if (this.groupBy != null) {
            throw RuntimeException("Group by part is already defined")
        }

        this.groupBy = columns

        return this
    }

    fun limit(limit: Int): Select {
        if (this.limit != null) {
            throw RuntimeException("Limit part is already defined")
        }

        if (limit < 0) {
            throw RuntimeException("Limit has to be a positive number")
        }

        this.limit = limit

        return this
    }

    fun orderBy(vararg order: Order): Select {
        if (this.orderBy != null) {
            throw RuntimeException("Order by part is already defined")
        }

        this.orderBy = order

        return this
    }

    fun having(condition: Condition): Select {
        if (this.having != null) {
            throw RuntimeException("Having part is already defined")
        }

        this.having = condition

        return this
    }

    override fun toSQL(): String {
        return "SELECT ${columns.joinToString(separator = ", ")} ${from!!.toSQL()}" +
                "${if (groupBy != null) " GROUP BY ${groupBy!!.joinToString(separator = ", ")}" else ""}" +
                "${if (having != null) " HAVING ${having!!.toSQL()}" else ""}" +
                "${if (orderBy != null) " ORDER BY ${orderBy!!.map { x -> x.toSQL() }.joinToString(separator = ", ")}" else ""}" +
                "${if (limit != null) " LIMIT $limit" else ""}"
    }

    override fun aS(alias: String): Select {
        this.alias = alias

        return this
    }

    override fun toString(): String {
        return if (alias != null) {
            "(${this.toSQL()}) as $alias"
        } else {
            this.toSQL()
        }
    }
}

open class Update(val table: Table) : Statement {
    private var condition: Condition? = null
    private val updates: MutableMap<Column, BaseValue> = mutableMapOf()

    fun where(condition: Condition): Update {
        if (this.condition != null) {
            throw RuntimeException("Condition for update is already defined")
        }

        this.condition = condition

        return this
    }

    fun set(column: Column, newValue: Column): Update {
        updates.put(column, newValue)

        return this
    }

    fun set(column: Column, newValue: Literal): Update {
        updates.put(column, newValue)

        return this
    }

    override fun toSQL(): String {
        if (updates.isEmpty()) {
            throw RuntimeException("There is no specified columns for update")
        }

        return "UPDATE $table " +
                "SET ${updates.map { x -> "${x.key} = ${x.value}" }.joinToString(separator = ", ")}" +
                " ${if (condition != null) "WHERE ${condition!!.toSQL()}" else ""}"
    }
}

open class Insert(val table: Table) : Statement {
    private var columnLst: Array<out Column>? = null
    private var values: List<out Literal> = emptyList()

    fun columns(vararg columns: Column): Insert {
        columnLst = columns

        return this
    }

    fun values(vararg values: Literal): Insert {
        this.values = values.asList()

        return this
    }

    override fun toSQL(): String {
        if (columnLst != null) {
            if (columnLst!!.size > values.size) {
                values = values.plus(Array(columnLst!!.size - values.size, { NULL() }))
            }

            if (columnLst!!.size < values.size) {
                throw RuntimeException("Values list for update is bigger than columns")
            }
        }


        return "INSERT INTO ${table.forAction()}" +
                "${if (columnLst!!.isNotEmpty()) "(${columnLst!!.map { x -> x.forAction() }.joinToString(separator = ", ")})" else ""} " +
                "VALUES(${values.joinToString(separator = ", ")})"
    }
}

open class Delete(val table: Table) : Statement {
    private var condition: Condition? = null

    fun where(condition: Condition): Delete {
        if (this.condition != null) {
            throw RuntimeException("Condition for delete is already defined")
        }

        this.condition = condition

        return this
    }

    override fun toSQL(): String = "DELETE FROM ${table.forAction()}" +
            " ${if (condition != null) "WHERE ${condition!!.toSQL()}" else ""}"
}

open class Create {
    fun table(schema: String? = null, name: String, caseSensitive: Boolean = false): CreateTable = CreateTable(schema, name, caseSensitive)

    fun index(name: String): CreateIndex = CreateIndex(name)

    open class CreateTable(schema: String? = null, name: String, caseSensitive: Boolean = false) : Statement {
        private val table: Table = Table(schema, name, caseSensitive)
        private val columns: MutableList<Pair<String, DataType>> = mutableListOf()
        private var primaryKey: PrimaryKey? = null
        private var foreignKeys: Array<out ForeignKey>? = null
        private var checks: Array<out Check>? = null

        fun addColumn(name: String, type: DataType): CreateTable {
            columns.add(Pair(name, type))

            return this
        }

        fun primaryKey(primaryKey: PrimaryKey): CreateTable {
            if (this.primaryKey != null) {
                throw RuntimeException("PK is already defined")
            }

            this.primaryKey = primaryKey

            return this
        }

        fun foreignKeys(foreignKeys: Array<ForeignKey>): CreateTable {
            if (this.foreignKeys != null) {
                throw RuntimeException("FKs are already defined")
            }

            this.foreignKeys = foreignKeys

            return this
        }

        fun checks(checks: Array<Check>): CreateTable {
            if (this.checks != null) {
                throw RuntimeException("Checks are already defined")
            }

            this.checks = checks

            return this
        }

        override fun toSQL(): String {
            if (columns.isEmpty()) {
                throw RuntimeException("Columns were not specified")
            }

            return "CREATE TABLE ${table.forAction()} (\n" +
                    "${columns.map { x -> "\t${x.first} ${x.second.toSQL()}" }.joinToString(separator = ", \n")}" +
                    "${if (primaryKey != null) ",\n${primaryKey!!.toSQL()}" else ""}" +
                    "${if (foreignKeys != null) ",\n${foreignKeys!!.map { x -> x.toSQL() }.joinToString(separator = ",\n")}" else ""}" +
                    "${if (checks != null) ",\n${checks!!.map { x -> x.toSQL() }.joinToString(separator = ",\n")}" else ""}" +
                    "\n)"
        }
    }

    // default index implementation
    open class CreateIndex(private val name: String) : Statement {
        private var table: Table? = null
        private var columns: Array<out Column>? = null

        fun forTable(table: Table): CreateIndex {
            if (this.table != null) {
                throw RuntimeException("Table is already defined")
            }

            this.table = table

            return this
        }

        fun forTable(schema: String? = null, name: String, caseSensitive: Boolean = false): CreateIndex {
            if (this.table != null) {
                throw RuntimeException("Table is already defined")
            }

            this.table = Table(schema, name, caseSensitive)

            return this
        }

        fun onColumns(vararg column: Column): CreateIndex {
            if (this.columns != null) {
                throw RuntimeException("Columns list is already defined")
            }

            this.columns = column

            return this
        }

        override fun toSQL(): String {
            if (table == null) {
                throw RuntimeException("Table was not defined")
            }

            if (columns == null || columns!!.isEmpty()) {
                throw RuntimeException("Columns list was not defined")
            }

            return "CREATE INDEX $name ON ${table!!.forAction()} (${columns!!.map { x -> x.forAction() }.joinToString(separator = ", ")})"
        }
    }
}

open class Truncate(val table: Table) : Statement {
    override fun toSQL(): String {
        return "TRUNCATE $table"
    }
}

open class Drop {
    fun table(table: Table): DropTable = DropTable(table)

    fun index(index: Index): DropIndex = DropIndex(index)

    open inner class DropIndex(private val index: Index) : Statement {
        override fun toSQL(): String = "DROP INDEX ${index.forAction()}"
    }

    open inner class DropTable(private val table: Table) : Statement {
        private var cascade: Boolean = false

        fun cascade(): DropTable {
            if (this.cascade) {
                throw RuntimeException("Cascade is already set to true")
            }

            this.cascade = true

            return this
        }

        override fun toSQL(): String {
            return "DROP TABLE $table ${if (cascade) "CASCADE" else "RESTRICT"}"
        }
    }
}

open class Alter {
    fun table(table: Table): AlterTable = AlterTable(table)

    open inner class AlterTable(private val table: Table) {
        fun addColumn(name: String, type: DataType) = object : Statement {
            override fun toSQL(): String = "ALTER TABLE ${table.forAction()} ADD $name ${type.toSQL()}"
        }

        fun alterColumn(name: String, type: DataType) = object : Statement {
            override fun toSQL(): String = "ALTER TABLE ${table.forAction()} ALTER COLUMN $name ${type.toSQL()}"
        }

        fun dropColumn(name: String) = object : Statement {
            override fun toSQL(): String = "ALTER TABLE ${table.forAction()} DROP COLUMN $name"
        }
    }
}

fun count(block: AggregationBuilder.() -> Unit): Aggregation = AggregationBuilder(AggregationType.COUNT).apply(block).build()

fun max(block: AggregationBuilder.() -> Unit): Aggregation = AggregationBuilder(AggregationType.MAX).apply(block).build()

fun min(block: AggregationBuilder.() -> Unit): Aggregation = AggregationBuilder(AggregationType.MIN).apply(block).build()

fun avg(block: AggregationBuilder.() -> Unit): Aggregation = AggregationBuilder(AggregationType.AVG).apply(block).build()

fun asc(block: OrderBuilder.() -> Unit): Order = OrderBuilder(OrderType.ASC).apply(block).build()

fun desc(block: OrderBuilder.() -> Unit): Order = OrderBuilder(OrderType.DESC).apply(block).build()

internal fun escapedValue(value: Any?): Any? {
    return value
}