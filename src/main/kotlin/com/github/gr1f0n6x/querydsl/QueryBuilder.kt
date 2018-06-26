package com.github.gr1f0n6x.querydsl

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

interface Statement: Clause

abstract class ForwardStatement<out T: Statement>(val statement: T): Statement

class Select(private vararg val columns: Any = arrayOf("*")) : Statement {
    private lateinit var from: From<Select>
    private var limit: Int? = null

    fun from(schema: String? = null, table: String): From<Select> {
        from = From(this, schema, table)

        return from
    }

    fun limit(limit: Int): Select {
        if (limit < 0) {
            throw RuntimeException("Limit has to be a positive number")
        }

        this.limit = limit

        return this
    }

    override fun build(): String {
        return "SELECT ${columns.joinToString(separator = ", ")} ${from.build()} ${if (limit != null) "LIMIT $limit" else ""}"
    }
}

class Update {}

class Insert {}

class Delete {}

class Create {}

class Truncate {}

class Drop {}

class Alter {}

class From<A : Statement>(val query: A, val schema: String?, val table: String) : ForwardStatement<A>(query) {
    constructor(query: A, table: String) : this(query, null, table)

    private var where: Array<out Condition>? = null

    fun innerJoin(schema: String? = null, table: String): Join<A> = Join(this, schema, table, JoinType.INNER)

    fun leftJoin(schema: String? = null, table: String): Join<A> = Join(this, schema, table, JoinType.LEFT)

    fun rightJoin(schema: String? = null, table: String): Join<A> = Join(this, schema, table, JoinType.RIGHT)

    fun fullJoin(schema: String? = null, table: String): Join<A> = Join(this, schema, table, JoinType.FULL)

    fun where(vararg condition: Condition): A {
        where = condition

        return query
    }

    override fun build(): String {
        return "FROM ${if (schema != null) schema + '.' else ""}$table ${if (where != null) "WHERE ${where!!.map { b -> "(${b.build()})" }.joinToString(separator = " OR ")}" else ""}"
    }
}

enum class JoinType {
    INNER, LEFT, RIGHT, FULL
}

class Join<A: Statement>(val clause: From<A>, val schema: String? = null, val table: String, val joinType: JoinType) : ForwardStatement<From<A>>(clause) {

    private lateinit var condition: Array<out Condition>

    fun on(vararg condition: Condition): From<A> {
        this.condition = condition

        return clause
    }

    override fun build(): String {
        TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
    }
}

class Where(vararg val condition: Condition) : Clause {
    override fun build(): String {
        TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
    }
}

enum class QueryType {
    DDL, DML
}

interface ClauseBuilder<out T : Clause> {
    fun build(): T
}

abstract class Aggregation<A>(val column: A) : Clause
abstract class Grouping<A>(val column: A) : Clause


abstract class Ordering<A>(val column: A) : Clause
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
    EQ("="), GE(">="), LE("<="), GT(">"), LT("<"), LIKE("LIKE"), IN("IN"), IS_NULL("IS NULL"), IS_NOT_NULL("IS NOT NULL"), NOT("NOT")
}

abstract class Condition(val conditionType: ConditionType, val and: Condition? = null) : Clause

class WrapperConditionBuilder(val conditionType: ConditionType) : ClauseBuilder<Condition> {
    lateinit var condition: Condition

    override fun build(): Condition = WrapperCondition(conditionType, condition)
}

class UnaryConditionBuilder(val conditionType: ConditionType) : ClauseBuilder<Condition> {
    lateinit var column: Any
    var and: Condition? = null

    override fun build(): Condition = UnaryCondition(conditionType, and, column)
}

class BinaryConditionBuilder(val conditionType: ConditionType) : ClauseBuilder<Condition> {
    lateinit var left: Any
    lateinit var right: Any
    var and: Condition? = null

    override fun build(): Condition = BinaryCondition(conditionType, and, left, right)
}

class TernaryConditionBuilder(val conditionType: ConditionType) : ClauseBuilder<Condition> {
    lateinit var left: Any
    lateinit var right: Any
    lateinit var mid: Any
    var and: Condition? = null

    override fun build(): Condition = TernaryCondition(conditionType, and, left, right, mid)
}

class WrapperCondition(conditionType: ConditionType, val condition: Condition): Condition(conditionType) {
    override fun build(): String {
        return "${conditionType.symbol} ${condition.build()}"
    }
}

class UnaryCondition(conditionType: ConditionType, and: Condition? = null, val column: Any) : Condition(conditionType, and) {
    override fun build(): String {
        return "$column ${conditionType.symbol} ${if (and != null) "AND ${and.build()}" else "" }"
    }
}

class BinaryCondition(conditionType: ConditionType, and: Condition? = null, val left: Any, val right: Any) : Condition(conditionType, and) {
    override fun build(): String {
        return "$left ${conditionType.symbol} $right ${if (and != null) "AND ${and.build()}" else "" }"
    }
}

class TernaryCondition(conditionType: ConditionType, and: Condition? = null, val left: Any, val right: Any, val mid: Any) : Condition(conditionType, and) {
    override fun build(): String {
        TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
    }
}

fun isNull(block: UnaryConditionBuilder.() -> Unit): Condition = UnaryConditionBuilder(ConditionType.IS_NULL).apply(block).build()

fun isNotNull(block: UnaryConditionBuilder.() -> Unit): Condition = UnaryConditionBuilder(ConditionType.IS_NOT_NULL).apply(block).build()

fun not(block: WrapperConditionBuilder.() -> Unit): Condition = WrapperConditionBuilder(ConditionType.NOT).apply(block).build()

fun eq(block: BinaryConditionBuilder.() -> Unit): Condition = BinaryConditionBuilder(ConditionType.EQ).apply(block).build()

fun ge(block: BinaryConditionBuilder.() -> Unit): Condition = BinaryConditionBuilder(ConditionType.GE).apply(block).build()

fun le(block: BinaryConditionBuilder.() -> Unit): Condition = BinaryConditionBuilder(ConditionType.LE).apply(block).build()

fun gt(block: BinaryConditionBuilder.() -> Unit): Condition = BinaryConditionBuilder(ConditionType.GT).apply(block).build()

fun lt(block: BinaryConditionBuilder.() -> Unit): Condition = BinaryConditionBuilder(ConditionType.LT).apply(block).build()

fun like(block: BinaryConditionBuilder.() -> Unit): Condition = BinaryConditionBuilder(ConditionType.LIKE).apply(block).build()

fun in_(block: BinaryConditionBuilder.() -> Unit): Condition = BinaryConditionBuilder(ConditionType.IN).apply(block).build()