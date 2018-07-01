package com.github.gr1f0n6x.querydsl

internal fun escapedValue(value: Any): Any {
    return value
}

data class Table(val schema: String? = null, val name: String, val alias: String? = null, val caseSensitive: Boolean = false) {
    override fun toString(): String {
        return if (caseSensitive) {
            "${if (schema != null) "\"$schema.\"" else ""}\"$name\"${if (alias != null) " $alias" else ""}"
        } else {
            "${if (schema != null) "$schema." else ""}$name${if (alias != null) " $alias" else ""}"
        }
    }

    internal fun forInsert(): String {
        return if (caseSensitive) {
            "${if (schema != null) "\"$schema.\"" else ""}\"$name\""
        } else {
            "${if (schema != null) "$schema." else ""}$name"
        }
    }
}

abstract class Value {
    protected var alias: String? = null

    fun between(lower: Value, upper: Value): Condition = TernaryConditionBuilder(ConditionType.BETWEEN).apply { left = lower; mid = this@Value; right = upper }.build()

    infix fun aS(alias: String): Value = this.apply { this.alias = alias }

    infix fun eq(value: Value): Condition = BinaryConditionBuilder(ConditionType.EQ).apply { left = this@Value; right = value }.build()

    infix fun ge(value: Value): Condition = BinaryConditionBuilder(ConditionType.GE).apply { left = this@Value; right = value }.build()

    infix fun le(value: Value): Condition = BinaryConditionBuilder(ConditionType.LE).apply { left = this@Value; right = value }.build()

    infix fun gt(value: Value): Condition = BinaryConditionBuilder(ConditionType.GT).apply { left = this@Value; right = value }.build()

    infix fun lt(value: Value): Condition = BinaryConditionBuilder(ConditionType.LT).apply { left = this@Value; right = value }.build()

    infix fun iS(value: Value): Condition = BinaryConditionBuilder(ConditionType.IS).apply { left = this@Value; right = value }.build()

    infix fun iSNot(value: Value): Condition = BinaryConditionBuilder(ConditionType.IS_NOT).apply { left = this@Value; right = value }.build()

    infix fun like(value: Value): Condition = BinaryConditionBuilder(ConditionType.LIKE).apply { left = this@Value; right = value }.build()

    infix fun iN(value: Value): Condition = BinaryConditionBuilder(ConditionType.IN).apply { left = this@Value; right = value }.build()
}

object NULL : Literal("NULL") {
    override fun toString(): String = "NULL"
}

data class Column(val name: String, val tableAlias: String? = null, val caseSensitive: Boolean = false) : Value() {
    override fun toString(): String {
        return if (caseSensitive) {
            "${if (tableAlias != null) "$tableAlias." else ""}\"$name\"${if (alias != null) " as $alias" else ""}"
        } else {
            "${if (tableAlias != null) "$tableAlias." else ""}$name${if (alias != null) " as $alias" else ""}"
        }
    }

    internal fun forInsert(): String {
        return if (caseSensitive) {
            "\"$name\""
        } else {
            name
        }
    }
}

//TODO: add basic literal types (numeric, char, varchar, etc.)  https://msdn.microsoft.com/en-us/library/office/ff195814.aspx
// Make this class abstract (or add implicit type conversion to concrete type?)
open class Literal(val literal: Any) : Value() {
    override fun toString(): String {
        return "${escapedValue(literal)}${if (alias != null) " as $alias" else ""}"
    }

    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (javaClass != other?.javaClass) return false

        other as Literal

        if (literal != other.literal) return false

        return true
    }

    override fun hashCode(): Int {
        return literal.hashCode()
    }
}

object QueryBuilder {
    fun select(vararg columns: Any): Select = Select(*columns)

    fun update(table: Table): Update = Update(table)

    fun insert(table: Table): Insert = Insert(table)

    fun delete(table: Table): Delete = Delete(table)

    fun create(): Create = Create()

    fun truncate(table: Table): Truncate = Truncate(table)

    fun drop(table: Table): Drop = Drop(table)

    fun alter(): Alter = Alter()
}

enum class StatementType(val type: QueryType) {
    INSERT(QueryType.DML), UPDATE(QueryType.DML), DELETE(QueryType.DML), SELECT(QueryType.DML),
    CREATE(QueryType.DDL), ALTER(QueryType.DDL), TRUNCATE(QueryType.DDL), DROP(QueryType.DDL),
    GRANT(QueryType.DDL)
}

interface Clause {
    fun build(): String
}

interface Statement : Clause

abstract class ForwardStatement<out T : Statement>(private val statement: T) : Statement

class Select(private vararg val columns: Any = arrayOf("*")) : Statement {
    private var from: From<Select>? = null
    private var limit: Int? = null
    private var groupBy: Array<out Column>? = null
    private var orderBy: Array<out Order>? = null
    private var having: Condition? = null

    fun from(table: Table): From<Select> {
        if (this.from != null) {
            throw RuntimeException("From part is already defined")
        }

        from = From(this, table)

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

    override fun build(): String {
        return "SELECT ${columns.joinToString(separator = ", ")} ${from!!.build()}" +
                " ${if (groupBy != null) "GROUP BY ${groupBy!!.joinToString(separator = ", ")}" else ""}" +
                " ${if (having != null) "HAVING ${having!!.build()}" else ""}" +
                " ${if (orderBy != null) "ORDER BY ${orderBy!!.map { x -> x.build() }.joinToString(separator = ", ")}" else ""}" +
                " ${if (limit != null) "LIMIT $limit" else ""}"
    }
}

class Update(val table: Table) : Statement {
    private var condition: Condition? = null
    private val updates: MutableMap<Column, Value> = mutableMapOf()

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

    override fun build(): String {
        if (updates.isEmpty()) {
            throw RuntimeException("There is no specified columns for update")
        }

        return "UPDATE $table " +
                "SET ${updates.map { x -> "${x.key} = ${x.value}" }.joinToString(separator = ", ")}" +
                " ${if (condition != null) "WHERE ${condition!!.build()}" else ""}"
    }
}

class Insert(val table: Table) : Statement {
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

    override fun build(): String {
        if (columnLst != null) {
            if (columnLst!!.size > values.size) {
                values = values.plus(Array(columnLst!!.size - values.size, { NULL }))
            }

            if (columnLst!!.size < values.size) {
                throw RuntimeException("Values list for update is bigger than columns")
            }
        }


        return "INSERT INTO ${table.forInsert()}" +
                "${if (columnLst!!.isNotEmpty()) "(${columnLst!!.map { x -> x.forInsert() }.joinToString(separator = ", ")})" else ""} " +
                "VALUES(${values.joinToString(separator = ", ")})"
    }
}

class Delete(val table: Table) : Statement {
    override fun build(): String {
        TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
    }
}

class Create {}

class Truncate(val table: Table) : Statement {
    override fun build(): String {
        return "TRUNCATE $table"
    }
}

class Drop(val table: Table) : Statement {
    private var cascade: Boolean = false

    fun cascade(): Drop {
        if (this.cascade) {
            throw RuntimeException("Cascade is already set to true")
        }

        this.cascade = true

        return this
    }

    override fun build(): String {
        return "DROP TABLE $table ${if (cascade) "CASCADE" else "RESTRICT"}"
    }
}

class Alter {}

class From<A : Statement>(private val query: A, private val table: Table) : ForwardStatement<A>(query) {

    private var joins: MutableList<Join<A>> = mutableListOf()
    private var where: Condition? = null

    fun innerJoin(table: Table): Join<A> = createJoin(table, JoinType.INNER)

    fun leftJoin(table: Table): Join<A> = createJoin(table, JoinType.LEFT)

    fun rightJoin(table: Table): Join<A> = createJoin(table, JoinType.RIGHT)

    fun fullJoin(table: Table): Join<A> = createJoin(table, JoinType.FULL)

    private fun createJoin(table: Table, joinType: JoinType): Join<A> {
        val join = Join(this, table, joinType)
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

    override fun build(): String = "FROM $table" +
            " ${if (joins.isNotEmpty()) "\n${joins.map { x -> x.build() }.joinToString(separator = "\n")}" else ""}" +
            " ${if (where != null) "\nWHERE ${where!!.build()}" else ""}"
}

enum class JoinType(val type: String) {
    INNER("INNER JOIN"), LEFT("LEFT JOIN"), RIGHT("RIGHT JOIN"), FULL("FULL JOIN")
}

class Join<A : Statement>(private val clause: From<A>, private val table: Table, private val joinType: JoinType) : ForwardStatement<From<A>>(clause) {

    private lateinit var condition: Condition

    fun on(condition: Condition): From<A> {
        this.condition = condition

        return clause
    }

    fun on(condition: String): From<A> {
        this.condition = RawCondition(condition)

        return clause
    }

    override fun build(): String = "${joinType.type} $table ON ${condition.build()}"
}

enum class QueryType {
    DDL, DML
}

interface ClauseBuilder<out T : Clause> {
    fun build(): T
}

enum class AggregationType(val symbol: String) {
    COUNT("COUNT"), MAX("MAX"), MIN("MIN"), AVG("AVG")
}

class Aggregation(private val aggregationType: AggregationType, private val column: Any) : Value(), Clause {
    override fun build(): String = "${aggregationType.symbol}($column)"

    override fun toString(): String = this.build()
}

class AggregationBuilder(private val aggregationType: AggregationType) : ClauseBuilder<Aggregation> {
    lateinit var column: Any

    override fun build(): Aggregation = Aggregation(aggregationType, column)
}

fun count(block: AggregationBuilder.() -> Unit): Aggregation = AggregationBuilder(AggregationType.COUNT).apply(block).build()

fun max(block: AggregationBuilder.() -> Unit): Aggregation = AggregationBuilder(AggregationType.MAX).apply(block).build()

fun min(block: AggregationBuilder.() -> Unit): Aggregation = AggregationBuilder(AggregationType.MIN).apply(block).build()

fun avg(block: AggregationBuilder.() -> Unit): Aggregation = AggregationBuilder(AggregationType.AVG).apply(block).build()

enum class OrderType(val symbol: String) {
    ASC("ASC"), DESC("DESC")
}

class Order(private val orderType: OrderType, private val column: Column) : Clause {
    override fun build(): String = "$column ${orderType.symbol}"
}

class OrderBuilder(private val orderType: OrderType) : ClauseBuilder<Order> {
    lateinit var column: Column

    override fun build(): Order = Order(orderType, column)
}

fun asc(block: OrderBuilder.() -> Unit): Order = OrderBuilder(OrderType.ASC).apply(block).build()

fun desc(block: OrderBuilder.() -> Unit): Order = OrderBuilder(OrderType.DESC).apply(block).build()

enum class ConditionType(val symbol: String) {
    EQ("="), GE(">="), LE("<="), GT(">"), LT("<"), LIKE("LIKE"), IN("IN"),
    NOT("NOT"), OR("OR"), AND("AND"), RAW(""), IS("IS"), IS_NOT("IS NOT"), BETWEEN("BETWEEN")
}

abstract class Condition(protected val conditionType: ConditionType) : Clause {
    infix fun or(condition: Condition): Condition = BinaryWrapperConditionBuilder(ConditionType.OR).apply { first = this@Condition; second = condition }.build()

    infix fun and(condition: Condition): Condition = BinaryWrapperConditionBuilder(ConditionType.AND).apply { first = this@Condition; second = condition }.build()

    operator fun not(): Condition = UnaryWrapperConditionBuilder(ConditionType.NOT).apply { condition = this@Condition }.build()
}

class RawCondition(private val clause: String) : Condition(ConditionType.RAW) {
    override fun build(): String = clause
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
    override fun build(): String = "${conditionType.symbol} ${condition.build()}"
}

class BinaryWrapperCondition(conditionType: ConditionType, private val first: Condition, private val second: Condition) : Condition(conditionType) {
    override fun build(): String = "(${first.build()} ${conditionType.symbol} ${second.build()})"
}

class UnaryCondition(conditionType: ConditionType, private val column: Value) : Condition(conditionType) {
    override fun build(): String = "$column ${conditionType.symbol}"
}

class BinaryCondition(conditionType: ConditionType, private val left: Value, private val right: Value) : Condition(conditionType) {
    override fun build(): String = "$left ${conditionType.symbol} $right"
}

class TernaryCondition(conditionType: ConditionType, private val left: Value, private val right: Value, private val mid: Value) : Condition(conditionType) {
    override fun build(): String = "$mid ${conditionType.symbol} $left AND $right"
}