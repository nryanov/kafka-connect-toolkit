package com.github.gr1f0n6x.querydsl

internal fun escapedValue(value: Any): Any {
    return value
}

internal fun caseSensitiveName(objectName: String): String {
    return """"$objectName""""
}

data class Table(val schema: String? = null, val name: String, val alias: String? = null, val caseSensitive: Boolean = false) {
    //TODO" case sensitive
    override fun toString(): String = "${if (schema != null) "$schema." else ""}$name${if (alias != null) " $alias" else ""}"
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

object NULL : Value() {
    override fun toString(): String = "NULL"
}

data class Column(val name: String, val tableAlias: String? = null, val caseSensitive: Boolean = false) : Value() {
    override fun toString(): String {
        if (caseSensitive) {
            return "${if (tableAlias != null) "$tableAlias." else ""}${caseSensitiveName(name)}${if (alias != null) " as $alias" else ""}"
        }

        return "${if (tableAlias != null) "$tableAlias." else ""}$name${if (alias != null) " as $alias" else ""}"
    }
}

data class Literal(val literal: Any) : Value() {
    override fun toString(): String {
        return "${escapedValue(literal)}${if (alias != null) " as $alias" else ""}"
    }
}

object QueryBuilder {
    fun select(vararg columns: Any): Select = Select(*columns)

    fun update(): Update = Update()

    fun insert(): Insert = Insert()

    fun delete(): Delete = Delete()

    fun create(): Create = Create()

    fun truncate(): Truncate = Truncate()

    fun drop(): Drop = Drop()

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

    fun from(schema: String? = null, table: String, alias: String? = null): From<Select> {
        if (this.from != null) {
            throw RuntimeException("From part is already defined")
        }

        from = From(this, schema, table, alias)

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

class Update {}

class Insert {}

class Delete {}

class Create {}

class Truncate : Statement {
    private var table: Table? = null

    fun table(table: Table): Truncate {
        if (this.table != null) {
            throw RuntimeException("Table for drop is already defined")
        }

        this.table = table

        return this
    }

    override fun build(): String {
        return "TRUNCATE $table"
    }
}

class Drop : Statement {
    private var table: Table? = null
    private var cascade: Boolean = false

    fun table(table: Table): Drop {
        if (this.table != null) {
            throw RuntimeException("Table for drop is already defined")
        }

        this.table = table

        return this
    }

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

class From<A : Statement>(private val query: A, private val schema: String? = null, private val table: String, private val alias: String? = null) : ForwardStatement<A>(query) {

    private var joins: MutableList<Join<A>> = mutableListOf()
    private var where: Condition? = null

    fun innerJoin(schema: String? = null, table: String, alias: String? = null): Join<A> = createJoin(schema, table, alias, JoinType.INNER)

    fun leftJoin(schema: String? = null, table: String, alias: String? = null): Join<A> = createJoin(schema, table, alias, JoinType.LEFT)

    fun rightJoin(schema: String? = null, table: String, alias: String? = null): Join<A> = createJoin(schema, table, alias, JoinType.RIGHT)

    fun fullJoin(schema: String? = null, table: String, alias: String? = null): Join<A> = createJoin(schema, table, alias, JoinType.FULL)

    private fun createJoin(schema: String? = null, table: String, alias: String? = null, joinType: JoinType): Join<A> {
        val join = Join(this, schema, table, alias, joinType)
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

    override fun build(): String = "FROM ${if (schema != null) "$schema." else ""}$table ${alias ?: ""}" +
            " ${if (joins.isNotEmpty()) "\n${joins.map { x -> x.build() }.joinToString(separator = "\n")}" else ""}" +
            " ${if (where != null) "\nWHERE ${where!!.build()}" else ""}"
}

enum class JoinType(val type: String) {
    INNER("INNER JOIN"), LEFT("LEFT JOIN"), RIGHT("RIGHT JOIN"), FULL("FULL JOIN")
}

class Join<A : Statement>(private val clause: From<A>, private val schema: String? = null, private val table: String, private val alias: String? = null, private val joinType: JoinType) : ForwardStatement<From<A>>(clause) {

    private lateinit var condition: Condition

    fun on(condition: Condition): From<A> {
        this.condition = condition

        return clause
    }

    fun on(condition: String): From<A> {
        this.condition = RawCondition(condition)

        return clause
    }

    override fun build(): String = "${joinType.type} ${if (schema != null) "$schema." else ""}$table ${alias ?: ""} ON ${condition.build()}"
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