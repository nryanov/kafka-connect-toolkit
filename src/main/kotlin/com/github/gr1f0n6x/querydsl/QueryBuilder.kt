package com.github.gr1f0n6x.querydsl

object QueryBuilder {
    fun select(vararg columns: String): Select = Select(*columns)

    fun update(): Update = Update()

    fun insert(): Insert = Insert()

    fun delete(): Delete = Delete()

    fun create(): Create = Create()

    fun truncate(): Truncate = Truncate()

    fun drop(): Drop = Drop()

    fun alter(): Alter = Alter()
}

interface Query {}

interface Clause {}

enum class QueryType {
    DDL, DML
}

enum class StatementType(val type: QueryType) {
    INSERT(QueryType.DML), UPDATE(QueryType.DML), DELETE(QueryType.DML), SELECT(QueryType.DML),
    CREATE(QueryType.DDL), ALTER(QueryType.DDL), TRUNCATE(QueryType.DDL), DROP(QueryType.DDL),
    GRANT(QueryType.DDL)
}

class Select(private vararg val columns: String): Query {
    constructor(): this("*")

    private var from: From<Select>? = null
    private var where: Where? = null
    private var orderBy: OrderBy? = null
    private var groupBy: GroupBy? = null
    private var limit: Int? = null

    fun from(table: String): From<Select> = From(this, table)

    fun from(schema: String, table: String): From<Select> = From(this, schema, table)

    fun orderBy(): OrderBy = OrderBy()

    fun having(): Having = Having()

    fun limit(limit: Int): Select = this
}

class Update {}

class Insert {}

class Delete {}

class Create {}

class Truncate {}

class Drop {}

class Alter {}

class From<A: Query>(val query: A, val schema: String?, val table: String): Clause {
    constructor(query: A, table: String): this(query, null, table)

    fun innerJoin(schema: String, table: String): InnerJoin<A> = InnerJoin(this, schema, table)

    fun innerJoin(table: String): InnerJoin<A> = InnerJoin(this, table)

    fun leftJoin(schema: String, table: String): LeftJoin<A> = LeftJoin(this, schema, table)

    fun leftJoin(table: String): LeftJoin<A> = LeftJoin(this, table)

    fun rightJoin(schema: String, table: String): RightJoin<A> = RightJoin(this, schema, table)

    fun rightJoin(table: String): RightJoin<A> = RightJoin(this, table)

    fun fullJoin(schema: String, table: String): FullJoin<A> = FullJoin(this, schema, table)

    fun fullJoin(table: String): FullJoin<A> = FullJoin(this, table)

    fun where(vararg condition: Condition): A {
        Where(this, *condition)

        return query
    }
}

class Where(val clause: Clause, vararg val condition: Condition): Clause

class OrderBy: Clause

class GroupBy: Clause

class Having: Clause

enum class JoinType {
    INNER, LEFT, RIGHT, FULL
}

abstract class Join<A: Query>(val clause: From<A>, val schema: String?, val table: String): Clause {
    private lateinit var condition: Array<out Condition>
    abstract val joinType: JoinType

    fun on(vararg condition: Condition): From<A> {
        this.condition = condition

        return clause
    }
}

class InnerJoin<A: Query>(clause: From<A>, schema: String?, table: String) : Join<A>(clause, schema, table) {
    constructor(clause: From<A>, table: String): this(clause, null, table)

    override val joinType: JoinType = JoinType.INNER
}

class LeftJoin<A: Query>(clause: From<A>, schema: String?, table: String) : Join<A>(clause, schema, table) {
    constructor(clause: From<A>, table: String): this(clause, null, table)

    override val joinType: JoinType = JoinType.LEFT
}

class RightJoin<A: Query>(clause: From<A>, schema: String?, table: String) : Join<A>(clause, schema, table) {
    constructor(clause: From<A>, table: String): this(clause, null, table)

    override val joinType: JoinType = JoinType.RIGHT
}

class FullJoin<A: Query>(clause: From<A>, schema: String?, table: String) : Join<A>(clause, schema, table) {
    constructor(clause: From<A>, table: String): this(clause, null, table)

    override val joinType: JoinType = JoinType.FULL
}

interface ClauseBuilder<out T: Clause> {
    fun build(): T
}

interface Condition: Clause

abstract class Ordering<A>(val column: A): Clause
abstract class OrderingBuilder<A>: ClauseBuilder<Ordering<A>> {
    var column: A? = null
}

class ASC(column: String?): Ordering<String?>(column)
class ASCBuilder: OrderingBuilder<String?>() {
    override fun build(): ASC = ASC(column)
}

class DESC(column: String?): Ordering<String?>(column)
class DESCBuilder: OrderingBuilder<String?>() {
    override fun build(): DESC = DESC(column)
}

abstract class Comparison<A, B>(val left: A, val right: B, val and: Comparison<A, B>? = null): Condition

interface ConditionBuilder<out T: Condition>: ClauseBuilder<T>

abstract class ComparisonBuilder<A, B>: ConditionBuilder<Comparison<A, B>> {
    var left: A? = null
    var right: B? = null
    var and: Comparison<A, B>? = null
}

class EQ(left: String?, right: String?, and: Comparison<String?, String?>?): Comparison<String?, String?>(left, right, and)
class EQBuilder: ComparisonBuilder<String?, String?>() {
    override fun build(): EQ = EQ(left, right, and)
}

class GE(left: String?, right: String?, and: Comparison<String?, String?>?): Comparison<String?, String?>(left, right, and)
class GEBuilder: ComparisonBuilder<String?, String?>() {
    override fun build(): GE = GE(left, right, and)
}

class LE(left: String?, right: String?, and: Comparison<String?, String?>?): Comparison<String?, String?>(left, right, and)
class LEBuilder: ComparisonBuilder<String?, String?>() {
    override fun build(): LE = LE(left, right, and)
}

class GT(left: String?, right: String?, and: Comparison<String?, String?>?): Comparison<String?, String?>(left, right, and)
class GTBuilder: ComparisonBuilder<String?, String?>() {
    override fun build(): GT = GT(left, right, and)
}

class LT(left: String?, right: String?, and: Comparison<String?, String?>?): Comparison<String?, String?>(left, right, and)
class LTBuilder: ComparisonBuilder<String?, String?>() {
    override fun build(): LT = LT(left, right, and)
}

class LIKE(left: String?, right: String?, and: Comparison<String?, String?>?): Comparison<String?, String?>(left, right, and)
class LIKEBuilder: ComparisonBuilder<String?, String?>() {
    override fun build(): LIKE = LIKE(left, right, and)
}

class IN(left: String?, right: String?, and: Comparison<String?, String?>?): Comparison<String?, String?>(left, right, and)
class INBuilder: ComparisonBuilder<String?, String?>() {
    override fun build(): IN = IN(left, right, and)
}

class BETWEEN(left: String?, right: String?, and: Comparison<String?, String?>?): Comparison<String?, String?>(left, right, and)
class BETWEENBuilder: ComparisonBuilder<String?, String?>() {
    override fun build(): BETWEEN = BETWEEN(left, right, and)
}


fun eq(block: EQBuilder.() -> Unit): EQ = EQBuilder().apply(block).build()

fun ge(block: GEBuilder.() -> Unit): GE = GEBuilder().apply(block).build()

fun le(block: LEBuilder.() -> Unit): LE = LEBuilder().apply(block).build()

fun gt(block: GTBuilder.() -> Unit): GT = GTBuilder().apply(block).build()

fun lt(block: LTBuilder.() -> Unit): LT = LTBuilder().apply(block).build()