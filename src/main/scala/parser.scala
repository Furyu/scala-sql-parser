package com.github.stephentu.scalasqlparser

import scala.util.matching.Regex

import scala.util.parsing.combinator._
import scala.util.parsing.combinator.lexical._
import scala.util.parsing.combinator.syntactical._
import scala.util.parsing.combinator.token._

import scala.util.parsing.input.CharArrayReader.EofCh
import org.slf4j.LoggerFactory

class SQLParser extends StandardTokenParsers {

  lazy val logger = LoggerFactory.getLogger(classOf[SQLParser])

  class NormalizingStringParsers(str: String) {
    /**
     * Parse a cAsE-iNsEnSiTiVe string and normalize the result to s lower-case string
     * @return a lower-case string
     */
    def ignoreCase = elem("keyword '" + str + "'", {
      elem => ("""(?i)\Q""" + str + """\E""").r.findFirstIn(elem.chars).isDefined
    }).map(_.chars.toLowerCase)
  }

  implicit def stringToComposableParsers(str: String): NormalizingStringParsers = new NormalizingStringParsers(str)

  class SqlLexical extends StdLexical {
    case class FloatLit(chars: String) extends Token {
      override def toString = chars
    }
    override def token: Parser[Token] =
      ( identChar ~ rep( identChar | digit )              ^^ { case first ~ rest => processIdent(first :: rest mkString "") }
      | '`' ~> ( identChar | digit ) ~ rep( identChar | digit ) <~ '`' ^^ { case first ~ rest => Identifier(first :: rest mkString "") }
      | rep1(digit) ~ opt('.' ~> rep(digit))              ^^ {
        case i ~ None    => NumericLit(i mkString "")
        case i ~ Some(d) => FloatLit(i.mkString("") + "." + d.mkString(""))
      }
      | '\'' ~ rep( chrExcept('\'', '\n', EofCh) ) ~ '\'' ^^ { case '\'' ~ chars ~ '\'' => StringLit(chars mkString "") }
      | '\"' ~ rep( chrExcept('\"', '\n', EofCh) ) ~ '\"' ^^ { case '\"' ~ chars ~ '\"' => StringLit(chars mkString "") }
      | EofCh                                             ^^^ EOF
      | '`' ~> failure("Unmatched backquotes")
      | '\'' ~> failure("unclosed string literal")
      | '\"' ~> failure("unclosed string literal")
      | delim
      | failure("illegal character")
      )
    def regex(r: Regex): Parser[String] = new Parser[String] {
      def apply(in: Input) = {
        val source = in.source
        val offset = in.offset
        val start = offset // handleWhiteSpace(source, offset)
        (r findPrefixMatchOf (source.subSequence(start, source.length))) match {
          case Some(matched) =>
            Success(source.subSequence(start, start + matched.end).toString,
                    in.drop(start + matched.end - offset))
          case None =>
            Success("", in)
        }
      }
    }
  }
  override val lexical = new SqlLexical

  def floatLit: Parser[String] =
    elem("decimal", _.isInstanceOf[lexical.FloatLit]) ^^ (_.chars)

  val functions = Seq("count", "sum", "avg", "min", "max", "substring", "extract")

  val keywords = Seq(
    "select", "as", "or", "and", "group", "order", "by", "where", "limit",
    "join", "asc", "desc", "from", "on", "not", "having", "distinct",
    "case", "when", "then", "else", "end", "for", "from", "exists", "between", "like", "in",
    "year", "month", "day", "null", "is", "date", "interval", "group", "order",
    "date", "time", "timestamp", "left", "right", "outer", "inner",
    "d", "t", "ts",
    "update", "set",
    "insert", "into", "values"
  )

  // TODO Make reserved words case-insensitive e.g. 'as', 'As', 'aS', 'AS' are reserved words.
  lexical.reserved ++= keywords ++ keywords.map(_.toUpperCase) ++ functions ++ functions.map(_.toUpperCase)

  lexical.delimiters += (
    "*", "+", "-", "<", "=", "<>", "!=", "<=", ">=", ">", "/", "(", ")", ",", ".", ";", "{", "}"
  )

  def select: Parser[SelectStmt] =
    "select".ignoreCase ~> projections ~
      opt(relations) ~ opt(filter) ~
      opt(groupBy) ~ opt(orderBy) ~ opt(limit) <~ opt(";") ^^ {
    case p ~ r ~ f ~ g ~ o ~ l => SelectStmt(p, r, f, g, o, l)
  }

  def ident_dot_opt_ident = ident ~ opt("." ~> ident)

  def field_ident: Parser[FieldIdent] = {
    ident_dot_opt_ident ^^ {
      case id ~ None => FieldIdent(None, id)
      case a ~ Some(b: String) => FieldIdent(Some(a), b)
    }
  }

  def assign: Parser[Assign] = field_ident ~ ("=" ~> expr) ^^ {
    case a ~ b => Assign(a, b)
  }

  def set: Parser[Seq[Assign]] = "set".ignoreCase ~> repsep(assign, ",")

  def update: Parser[UpdateStmt] =
    "update".ignoreCase ~> rep1sep(relation, ",") ~ set ~ opt(filter) <~ opt(";") ^^ {
      case r ~ e ~ f => UpdateStmt(r, e, f)
    }

  def ins_row: Parser[InsRow] =
    (
      "values".ignoreCase ~> "(" ~> repsep(expr, ",") <~ ")" ^^ { case exprs => Values(exprs.map(e => PositionalValueBinding(e))) }
    ) | (
      "(" ~> rep1sep(ident_dot_opt_ident, ",") ~ ")" ~ "values".ignoreCase ~ "(" ~ repsep(expr, ",") <~ ")" ^^ {
        case fields ~ _ ~ _ ~ _ ~ exprs =>
          NamedValues(
            fields.zip(exprs).map {
              case (f, e) =>
                f match {
                  case id ~ None =>
                    NamedValueBinding(e, ColumnFieldIdent(None, id))
                  case a ~ Some(b) =>
                    NamedValueBinding(e, ColumnFieldIdent(Some(a), b))
                }

            }
          )
      }
    ) | (
      set ^^ { case assigns => Set(assigns) }
    ) |
    failure("ins_row expected")

  def insert: Parser[InsertStmt] =
    "insert".ignoreCase ~> "into".ignoreCase ~> (ident  ^^ { case ident => TableRelationAST(ident, None) }) ~ ins_row <~ opt(";") ^^ {
      case i ~ r => InsertStmt(i, r)
    }

  def projections: Parser[Seq[SqlProj]] = repsep(projection, ",")

  def projection: Parser[SqlProj] =
    "*" ^^ (_ => StarProj()) |
    expr ~ opt("as".ignoreCase ~> ident) ^^ {
      case expr ~ ident => ExprProj(expr, ident)
    } |
    failure("projection expected")

  def expr: Parser[SqlExpr] = or_expr

  def or_expr: Parser[SqlExpr] =
    and_expr * ( "or".ignoreCase ^^^ { (a: SqlExpr, b: SqlExpr) => Or(a, b) } )

  def and_expr: Parser[SqlExpr] =
    cmp_expr * ( "and".ignoreCase ^^^ { (a: SqlExpr, b: SqlExpr) => And(a, b) } )

  // TODO: this function is nasty- clean it up!
  def cmp_expr: Parser[SqlExpr] =
    add_expr ~ rep(
      (
        "is".ignoreCase ~ opt("not".ignoreCase) ~ "null".ignoreCase ^^ {
          case is ~ not ~ nul => is + not.map(" " +).getOrElse("") + " " + nul
        } |
        ("=" | "<>" | "!=" | "<" | "<=" | ">" | ">=") ~ add_expr ^^ {
          case op ~ rhs => (op, rhs)
        } |
        "between".ignoreCase ~ add_expr ~ "and".ignoreCase ~ add_expr ^^ {
          case op ~ a ~ _ ~ b => (op, a, b)
        } |
        opt("not".ignoreCase) ~ "in".ignoreCase ~ "(" ~ (select | rep1sep(expr, ",")) ~ ")" ^^ {
          case n ~ op ~ _ ~ a ~ _ => (op, a, n.isDefined)
        } |
        opt("not".ignoreCase) ~ "like".ignoreCase ~ add_expr ^^ { case n ~ op ~ a => (op, a, n.isDefined) }
      )
    ) ^^ {
        case lhs ~ elems =>
          elems.foldLeft(lhs) {
            case (acc, (("=", rhs: SqlExpr))) => Eq(acc, rhs)
            case (acc, (("<>", rhs: SqlExpr))) => Neq(acc, rhs)
            case (acc, (("!=", rhs: SqlExpr))) => Neq(acc, rhs)
            case (acc, (("<", rhs: SqlExpr))) => Lt(acc, rhs)
            case (acc, (("<=", rhs: SqlExpr))) => Le(acc, rhs)
            case (acc, ((">", rhs: SqlExpr))) => Gt(acc, rhs)
            case (acc, ((">=", rhs: SqlExpr))) => Ge(acc, rhs)
            case (acc, (("between", l: SqlExpr, r: SqlExpr))) => And(Ge(acc, l), Le(acc, r))
            case (acc, (("in", e: Seq[_], n: Boolean))) => In(acc, e.asInstanceOf[Seq[SqlExpr]], n)
            case (acc, (("in", s: SelectStmt, n: Boolean))) => In(acc, Seq(Subselect(s)), n)
            case (acc, (("like", e: SqlExpr, n: Boolean))) => Like(acc, e, n)
            case (acc, "is null") => IsNull(acc)
            case (acc, "is not null") => IsNotNull(acc)
          }
      } |
    "not".ignoreCase ~> cmp_expr ^^ (Not(_)) |
    "exists".ignoreCase ~> inParenthesis(select) ^^ { case s => Exists(Subselect(s)) } |
    failure("cmp_expr expected")

  def inParenthesis[A](inner: Parser[A]): Parser[A] =
    "(" ~> inParenthesis(inner) <~ (")" | failure("Unmatched closing parenthesis")) |
    inner

  def add_expr: Parser[SqlExpr] =
    mult_expr * (
      "+" ^^^ { (a: SqlExpr, b: SqlExpr) => Plus(a, b) } |
      "-" ^^^ { (a: SqlExpr, b: SqlExpr) => Minus(a, b) } )

  def mult_expr: Parser[SqlExpr] =
    primary_expr * (
      "*" ^^^ { (a: SqlExpr, b: SqlExpr) => Mult(a, b) } |
      "/" ^^^ { (a: SqlExpr, b: SqlExpr) => Div(a, b) } )

  def primary_expr: Parser[SqlExpr] =
    literal |
    known_function |
    ident ~ opt( "." ~> ident  | "(" ~> repsep(expr, ",") <~ ")" ) ^^ {
      case id ~ None => FieldIdent(None, id)
      case a ~ Some( b: String ) => FieldIdent(Some(a), b)
      case a ~ Some( xs: Seq[_] ) => FunctionCall(a, xs.asInstanceOf[Seq[SqlExpr]])
    } |
    "(" ~> (expr | select ^^ (Subselect(_))) <~ ")" |
    "+" ~> primary_expr ^^ (UnaryPlus(_)) |
    "-" ~> primary_expr ^^ (UnaryMinus(_)) |
    case_expr |
    failure("primary_expr expected")

  def case_expr: Parser[SqlExpr] =
    "case".ignoreCase ~>
      opt(expr) ~ rep1("when".ignoreCase ~> expr ~ "then".ignoreCase ~ expr ^^ { case a ~ _ ~ b => CaseExprCase(a, b) }) ~
      opt("else".ignoreCase ~> expr) <~ "end".ignoreCase ^^ {
      case Some(e) ~ cases ~ default => CaseExpr(e, cases, default)
      case None ~ cases ~ default => CaseWhenExpr(cases, default)
    }

  def known_function: Parser[SqlExpr] =
    "count" ~> "(" ~> ( "*" ^^ (_ => CountStar()) | opt("distinct".ignoreCase) ~ expr ^^ { case d ~ e => CountExpr(e, d.isDefined) }) <~ ")" |
    "min".ignoreCase ~> "(" ~> expr <~ ")" ^^ (Min(_)) |
    "max".ignoreCase ~> "(" ~> expr <~ ")" ^^ (Max(_)) |
    "sum".ignoreCase ~> "(" ~> (opt("distinct".ignoreCase) ~ expr) <~ ")" ^^ { case d ~ e => Sum(e, d.isDefined) } |
    "avg".ignoreCase ~> "(" ~> (opt("distinct".ignoreCase) ~ expr) <~ ")" ^^ { case d ~ e => Avg(e, d.isDefined) } |
    "extract".ignoreCase ~> "(" ~ ("year".ignoreCase | "month".ignoreCase | "day".ignoreCase) ~ "from".ignoreCase ~ expr ~ ")" ^^ {
      case _ ~ "year" ~ _ ~ e ~ _ => Extract(e, YEAR)
      case _ ~ "month" ~ _ ~ e ~ _ => Extract(e, MONTH)
      case _ ~ "day" ~ _ ~ e ~ _ => Extract(e, DAY)
    } |
    "substring" ~> "(" ~> ( expr ~ "from" ~ numericLit ~ opt("for" ~> numericLit) ) <~ ")" ^^ {
      case e ~ "from" ~ a ~ b => Substring(e, a.toInt, b.map(_.toInt))
    } |
    failure("known_function expected")

  override def stringLit: Parser[String] = inParenthesis(super.stringLit)

  def literal: Parser[SqlExpr] = inParenthesis(
    numericLit ^^ { case i => IntLiteral(BigInt(i)) } |
    floatLit ^^ { case f => FloatLiteral(f.toDouble) } |
    stringLit ^^ { case s => StringLiteral(s) } |
    "null".ignoreCase ^^ (_ => NullLiteral()) |
    "date".ignoreCase ~> stringLit ^^ (DateLiteral(_)) |
    "time".ignoreCase ~> stringLit ^^ (TimeLiteral(_)) |
    "timestamp".ignoreCase ~> stringLit ^^ (TimestampLiteral(_)) |
    "interval".ignoreCase ~> stringLit ~ ("year" ^^^ (YEAR) | "month" ^^^ (MONTH) | "day" ^^^ (DAY)) ^^ {
      case d ~ u => IntervalLiteral(d, u)
    } |
    odbcDateAndTimeLiterals
  )

  def odbcDateAndTimeLiterals: Parser[SqlExpr] =
    "{" ~> ("d" | "t" | "ts") ~ stringLit <~ "}" ^^ {
      case "d" ~ str => ODBCDateLiteral(str)
      case "t" ~ str => ODBCTimeLiteral(str)
      case "ts" ~ str => ODBCTimestampLiteral(str)
    }

  def relations: Parser[Seq[SqlRelation]] = "from".ignoreCase ~> rep1sep(relation, ",")

  def relation: Parser[SqlRelation] = inParenthesis(
    simple_relation ~ rep(opt(join_type) ~ "join".ignoreCase ~ simple_relation ~ "on".ignoreCase ~ expr ^^
      { case tpe ~ _ ~ r ~ _ ~ e => (tpe.getOrElse(InnerJoin), r, e)}) ^^ {
      case r ~ elems => elems.foldLeft(r) { case (x, r) => JoinRelation(x, r._2, r._1, r._3) }
    }
  )

  def join_type: Parser[JoinType] =
    ("left".ignoreCase | "right".ignoreCase) ~ opt("outer".ignoreCase) ^^ {
      case "left" ~ o  => LeftJoin
      case "right" ~ o => RightJoin
    } |
    "inner".ignoreCase ^^^ (InnerJoin)

  def simple_relation: Parser[SqlRelation] =
    ident ~ opt("as".ignoreCase) ~ opt(ident) ^^ {
      case ident ~ _ ~ alias => TableRelationAST(ident, alias)
    } |
    "(" ~ select ~ ")" ~ opt("as".ignoreCase) ~ ident ^^ {
      case _ ~ select ~ _ ~ _ ~ alias => SubqueryRelationAST(select, alias)
    } |
    failure("No simple_relation found")

  def filter: Parser[SqlExpr] = "where".ignoreCase ~> expr

  def groupBy: Parser[SqlGroupBy] =
    "group".ignoreCase ~> "by".ignoreCase ~> rep1sep(expr, ",") ~ opt("having".ignoreCase ~> expr) ^^ {
      case k ~ h => SqlGroupBy(k, h)
    }

  def orderBy: Parser[SqlOrderBy] =
    "order".ignoreCase ~> "by".ignoreCase ~> rep1sep( expr ~ opt("asc".ignoreCase | "desc".ignoreCase) ^^ {
      case i ~ (Some("asc") | None) => (i, ASC)
      case i ~ Some("desc") => (i, DESC)
    }, ",") ^^ (SqlOrderBy(_))

  def limit: Parser[Int] = "limit".ignoreCase ~> numericLit ^^ (_.toInt)

  private def stripQuotes(s:String) = s.substring(1, s.length-1)

  def parse(sql: String): Option[Stmt] = {
    parseOrError(sql)
      .right.map { stmt =>
        Some(stmt)
      }
      .left.map { e =>
        logger.warn("Parse failed", e)
        None
      }
      .merge
  }

  /**
   * Parse the given sql as the `parse` method do, but returns Either[Throwable, Stmt] instead of Option[Stmt]
   * @param sql
   * @return
   */
  def parseOrError(sql: String): Either[Throwable, Stmt] = {
    phrase(select | insert | update | failure("no select/insert/update query found"))(new lexical.Scanner(sql)) match {
      case Success(r, q) => Right(r)
      case ns : NoSuccess => Left(new Exception("NoSuccess: \n" + ns))
    }
  }
}
