import itertools
from enum import Enum

from common import is_str_char


class SqlToken(object):
    value: str

    def __init__(self, value: str):
        self.value = value.strip()

    def __str__(self):
        return self.value


class SqlStarToken(SqlToken):
    ...


class SqlSelectToken(SqlToken):
    ...


class SqlFromToken(SqlToken):
    ...


class SqlWhereToken(SqlToken):
    ...


class SqlLiteralToken(SqlToken):
    def __init__(self, value: str):
        self.value = value.strip().replace("[", "").replace("]", "")


class SqlNameToken(SqlToken):

    def __init__(self, value:str):
        self.value = value.strip().replace("[", "").replace("]", "")


class SqlNotToken(SqlToken):
    ...


class SqlOperatorToken(SqlToken):
    ...


class SqlBooleanOperatorToken(SqlToken):
    ...

class SqlParseState(Enum):
    Initial = 1
    Select = 2
    From = 3
    Where = 4

class Parser(object):
    """
    select_statement ::= SELECT [* | <field_list>] FROM <tables> [WHERE <expression>]
    field_list ::= <field_entry> [',' <field_list>]
    field_entry ::= <name> [AS <literal>]
    name ::=  <bracketed_name> ['.' <bracketed_name> ['.' <bracketed_name>]]
    bracketed_name ::= ['['] <literal> [']']
    tables ::= <name> [',' <tables>]
    expression ::= ['('] <term> [AND | OR <expression>] [')']
    term ::= ['('] <field_entry> | <literal> [NOT] <operator> <field_entry> | <literal> [')']
    operator ::= '=' | '<' | '>' | '<>' | 'IS' | 'IS NOT'
    """

    tokens: list[SqlToken]
    sql: str
    index: int
    state:SqlParseState

    def __init__(self, sql: str):
        self.tokens = []
        self.sql = sql
        self.index = 0
        self.state = SqlParseState.Initial

        while self.index < len(sql):
            c = self.sql[self.index]
            if c.isspace():
                self.index += 1
            elif c.isdecimal():
                token = self.parse_number()
                self.tokens.append(token)
            elif is_str_char(c):
                token = self.parse_string()
                self.tokens.append(token)
            elif c == "*":
                self.tokens.append(SqlStarToken(c))
                self.index += 1
            elif c == "=" or c == ">" or c == "<":
                self.tokens.append(SqlOperatorToken(c))
                self.index += 1
            else:
                token = self.parse_string()
                self.tokens.append(token)
                self.index += 1

        self.clean_tokens()

    def parse_number(self) -> SqlToken:
        c = self.sql[self.index]
        str = ""
        while c.isdecimal():
            str += c
            self.index += 1
            c = self.sql[self.index]

        return SqlLiteralToken(str)

    def parse_string(self):
        c = self.sql[self.index]
        str = ""
        while (is_str_char(c)) and self.index < len(self.sql):
            str += c
            self.index += 1
            if self.index < len(self.sql):
                c = self.sql[self.index]

        str_upper = str.upper().strip()
        if str_upper == "SELECT":
            self.state = SqlParseState.Select
            return SqlSelectToken(str)
        elif str_upper == "WHERE":
            self.state = SqlParseState.Where
            return SqlWhereToken(str)
        elif str_upper == "FROM":
            self.state = SqlParseState.From
            return SqlFromToken(str)
        elif str_upper == "NOT":
            return SqlNotToken(str)
        elif str_upper == "AND":
            return SqlBooleanOperatorToken(str)
        elif str_upper == "OR":
            return SqlBooleanOperatorToken(str)
        elif self.state == SqlParseState.Select or self.state == SqlParseState.From or "'" not in str:
            return SqlNameToken(str)
        else:
            return SqlLiteralToken(str)

    def clean_tokens(self):
        # combine operators
        new_tokens: list[SqlToken] = []
        grouped = []
        key_func = lambda x: type(x)
        for key, group in itertools.groupby(self.tokens, key_func):
            grouped.append(list(group))

        for group in grouped:
            if type(group[0]) == SqlOperatorToken:
                new_token = SqlOperatorToken("".join([t.value for t in group]))
                new_tokens.append(new_token)
            else:
                new_tokens.extend(group)

        # for i in range(len(new_tokens)):
        #     token = new_tokens[i]
        #     if token is SqlLiteralToken:

        # check for normal tokens
        # for i in range(len(new_tokens)):
        #     token = new_tokens[i]
        #     if token is SqlLiteralToken:
        #         str_upper = token.value.upper().strip()
        #         if str_upper == "SELECT":
        #             new_tokens[i] = SqlSelectToken(token.value)
        #         elif str_upper == "WHERE":
        #             new_tokens[i] = SqlWhereToken(token.value)
        #         elif str_upper == "FROM":
        #             new_tokens[i] = SqlFromToken(token.value)
        #         elif str_upper == "NOT":
        #             new_tokens[i] = SqlNotToken(token.value)
        #         elif str_upper == "AND":
        #             new_tokens[i] = SqlBooleanOperatorToken(token.value)
        #         elif str_upper == "OR":
        #             new_tokens[i] = SqlBooleanOperatorToken(token.value)

        self.tokens = new_tokens
