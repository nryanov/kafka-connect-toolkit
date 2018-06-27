package com.github.gr1f0n6x.querydsl

import sun.reflect.CallerSensitive

internal fun escapedValue(value: Any): Any {
    return value
}

internal fun caseSensitiveName(objectName: String): String {
    return """"$objectName""""
}

data class Column(val name: String, val alias: String? = null, val caseSensitive: Boolean = false) {
    override fun toString(): String {
        if (caseSensitive) {
            return "${if (alias != null) "$alias." else ""}${caseSensitiveName(name)}"
        }

        return "${if (alias != null) "$alias." else ""}$name"
    }
}

data class Literal(val literal: Any) {
    override fun toString(): String {
        return "${escapedValue(literal)}"
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

    fun from(schema: String? = null, table: String, alias: String? = null): From<Select> {
        if (this.from != null) {
            throw RuntimeException("From part is already defined")
        }

        from = From(this, schema, table, alias)

        return from as From<Select>
    }

    fun groupBy(vararg columns: Column): Statement {
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

    override fun build(): String {
        return "SELECT ${columns.joinToString(separator = ", ")} ${from!!.build()}" +
                " ${if (groupBy != null) "GROUP BY ${groupBy!!.joinToString(separator = ", ")}" else ""}" +
                " ${if (limit != null) "LIMIT $limit" else ""}"
    }
}

class Update {}

class Insert {}

class Delete {}

class Create {}

class Truncate {}

class Drop {}

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

    override fun build(): String {
        return "FROM ${if (schema != null) "$schema." else ""}$table ${alias ?: ""}" +
                " ${if (joins.isNotEmpty()) "\n${joins.map { x -> x.build() }.joinToString(separator = "\n")}" else ""}" +
                " ${if (where != null) "\nWHERE ${where!!.build()}" else ""}"
    }
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

    override fun build(): String {
        return "${joinType.type} ${if (schema != null) "$schema." else ""}$table ${alias ?: ""} ON ${condition.build()}"
    }
}

enum class QueryType {
    DDL, DML
}

interface ClauseBuilder<out T : Clause> {
    fun build(): T
}

abstract class Aggregation<A>(private val column: A) : Clause

abstract class Ordering<A>(private val column: A) : Clause
abstract class OrderingBuilder<A> : ClauseBuilder<Ordering<A>> {
    var column: A? = null
}

class ASC(column: String?) : Ordering<String?>(column) {
    override fun build(): String {
        TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
    }
}

class ASCBuilder : OrderingBuilder<String?>() {
    override fun build(): ASC = ASC(column)
}

class DESC(column: String?) : Ordering<String?>(column) {
    override fun build(): String {
        TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
    }
}

class DESCBuilder : OrderingBuilder<String?>() {
    override fun build(): DESC = DESC(column)
}

enum class ConditionType(val symbol: String) {
    EQ("="), GE(">="), LE("<="), GT(">"), LT("<"), LIKE("LIKE"), IN("IN"), IS_NULL("IS NULL"), IS_NOT_NULL("IS NOT NULL"),
    NOT("NOT"), OR("OR"), AND("AND"), RAW("")
}

abstract class Condition(protected val conditionType: ConditionType) : Clause

class RawCondition(private val clause: String): Condition(ConditionType.RAW) {
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
    lateinit var column: Any

    override fun build(): Condition = UnaryCondition(conditionType, column)
}

class BinaryConditionBuilder(private val conditionType: ConditionType) : ClauseBuilder<Condition> {
    lateinit var left: Any
    lateinit var right: Any

    override fun build(): Condition = BinaryCondition(conditionType, left, right)
}

class TernaryConditionBuilder(private val conditionType: ConditionType) : ClauseBuilder<Condition> {
    lateinit var left: Any
    lateinit var right: Any
    lateinit var mid: Any

    override fun build(): Condition = TernaryCondition(conditionType, left, right, mid)
}

class UnaryWrapperCondition(conditionType: ConditionType, private val condition: Condition) : Condition(conditionType) {
    override fun build(): String {
        return "${conditionType.symbol} ${condition.build()}"
    }
}

class BinaryWrapperCondition(conditionType: ConditionType, private val first: Condition, private val second: Condition) : Condition(conditionType) {
    override fun build(): String {
        return "(${first.build()} ${conditionType.symbol} ${second.build()})"
    }
}

class UnaryCondition(conditionType: ConditionType, private val column: Any) : Condition(conditionType) {
    override fun build(): String {
        return "$column ${conditionType.symbol}"
    }
}

class BinaryCondition(conditionType: ConditionType, private val left: Any, private val right: Any) : Condition(conditionType) {
    override fun build(): String {
        return "$left ${conditionType.symbol} $right"
    }
}

class TernaryCondition(conditionType: ConditionType, private val left: Any, private val right: Any, private val mid: Any) : Condition(conditionType) {
    override fun build(): String {
        TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
    }
}

fun isNull(block: UnaryConditionBuilder.() -> Unit): Condition = UnaryConditionBuilder(ConditionType.IS_NULL).apply(block).build()

fun isNotNull(block: UnaryConditionBuilder.() -> Unit): Condition = UnaryConditionBuilder(ConditionType.IS_NOT_NULL).apply(block).build()

fun not(block: UnaryWrapperConditionBuilder.() -> Unit): Condition = UnaryWrapperConditionBuilder(ConditionType.NOT).apply(block).build()

fun or(block: BinaryWrapperConditionBuilder.() -> Unit): Condition = BinaryWrapperConditionBuilder(ConditionType.OR).apply(block).build()

fun and(block: BinaryWrapperConditionBuilder.() -> Unit): Condition = BinaryWrapperConditionBuilder(ConditionType.AND).apply(block).build()

fun eq(block: BinaryConditionBuilder.() -> Unit): Condition = BinaryConditionBuilder(ConditionType.EQ).apply(block).build()

fun ge(block: BinaryConditionBuilder.() -> Unit): Condition = BinaryConditionBuilder(ConditionType.GE).apply(block).build()

fun le(block: BinaryConditionBuilder.() -> Unit): Condition = BinaryConditionBuilder(ConditionType.LE).apply(block).build()

fun gt(block: BinaryConditionBuilder.() -> Unit): Condition = BinaryConditionBuilder(ConditionType.GT).apply(block).build()

fun lt(block: BinaryConditionBuilder.() -> Unit): Condition = BinaryConditionBuilder(ConditionType.LT).apply(block).build()

// TODO: escape clause
fun like(block: BinaryConditionBuilder.() -> Unit): Condition = BinaryConditionBuilder(ConditionType.LIKE).apply(block).build()

fun in_(block: BinaryConditionBuilder.() -> Unit): Condition = BinaryConditionBuilder(ConditionType.IN).apply(block).build()