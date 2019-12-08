from enum import Enum

relmap = {'!=':lambda a,b : a!=b,
          '>=':lambda a,b : a>=b,
          '<=':lambda a,b : a<=b,
          '>' :lambda a,b : a>b,
          '<' :lambda a,b : a<b,
          '=' :lambda a,b : a==b}

relops = sorted(relmap.keys(),key = lambda x : (len(x),x), reverse = True)

arithmap = {'+':lambda a,b : a+b,
            '-':lambda a,b : a-b,
            '*':lambda a,b : a*b,
            '/':lambda a,b : a/b}

arithops = arithmap.keys()

class TypedFunction :
    """
    Function that has kind and val atrributes
    """
    def __init__(self, f, kind, val) :
        self.f = f
        self.kind = kind
        self.val = val

    def __call__(self,*args,**kwargs) :
        return self.f(*args,**kwargs)
    def __str__(self) :
        return f'TypedFunction({self.kind},{self.val})'
    def __repr__(self) :
        return str(self)

def get_col(s,tables,names) :
    """
    Given a field name, a list of tables, and their names,
    retrieves the column corresponding to the field.
    Input:
    s - field name
    tables - list of tables
    names - list of names of the corresponding tables
    Output:
    table column corresponding to field name
    """
    isJoin = len(tables) == 2
    if isJoin :
        tname,fname = s.split('.')
        table_idx = names.index(tname)
    else :
        fname = s
        table_idx = 0
    col = tables[table_idx].namemap[fname]
    return col,table_idx

def get_getter(s,tables,names) :
    """
    Given a string s of the form #, or colname, or colname op #
    returns a function that produces the row value from a row index
    (or pair of row indices if it is a join).
    Input:
    s - string of an expression as described above
    Output:
    function that takes a row index and produces the expression value
    """
    if not s[0].isalpha() :
        v = float(s)
        return TypedFunction(lambda i,j : v,0,v)

    op_idxs = [s.find(a) for a in arithops if s.find(a)>-1]
    if len(op_idxs) == 0 :
        col,table_idx = get_col(s,tables,names)
        return TypedFunction(lambda i,j : col[ [i,j][table_idx] ],1,s)
    op_idx = min(op_idxs)
    op_fun = arithmap[s[op_idx]]
    a_str = s[0:op_idx].strip()
    b_str = s[op_idx+1:].strip()
    v = float(b_str)
    col,table_idx = get_col(a_str,tables,names)
    return TypedFunction(lambda i,j : op_fun(col[ [i,j][table_idx] ],v),2,v)

class Condition :
    """
    Evaluates a join or select condition
    """
    def __init__(self,s,tables,names) :
        """
        Constructs a condition from a string s.
        Inputs:
        s - string describing the join or select condition
        """
        self.s = s
        self.prepare(tables,names)
        
    def prepare(self,tables,names) :
        """
        Given a list of tables and their names, prepares the condition
        to quickly be evaluated.
        Inputs:
        tables - list of tables
        names - list of names of the corresponding tables
        """
        self.names = names
        self.tables = tables
        s = self.s
        logic = ' or ' if ' or ' in s else ' and '
        self.isAnd = logic == ' and '
        parts = [t.strip() for t in s.split(logic)]
        self.funs = []
        self.info = []
        for p in parts :
            if p[0] == '(' :
                p = p[1:-1].strip()
            for r in relops :
                if r in p :
                    rel = r
                    ptoks = [t.strip() for t in p.split(r)]
                    break
            cmp_fun = relmap[rel]
            a = get_getter(ptoks[0],tables,names)
            b = get_getter(ptoks[1],tables,names)
            f = lambda i,j : cmp_fun(a(i,j),b(i,j))
            self.funs.append(f)
            self.info.append( (rel,a,b) )

    def get_select_index(self) :
        """
        Returns (fieldName,value) if this condition has an
        equality condition, or None otherwise.
        """
        for rel,a,b in self.info :
            if rel == '=' :
                if a.kind == 1 and b.kind == 0 and a.val in self.tables[0].indexes:
                    return (a.val,b.val)
                elif b.kind == 1 and a.kind == 0 and b.val in self.tables[0].indexes:
                    return (b.val,a.val)
        return None
                
    def get_join_index(self) :
        """
        Returns (fieldName,fieldName) if this condition has an
        equality condition, or None otherwise.
        """
        for rel,a,b in self.info :
            if rel == '=' :
                if a.kind == 1 and b.kind == 1:
                    a_tname,a_fname = a.val.split('.')
                    b_tname,b_fname = b.val.split('.')
                    if self.names[0] == a_tname and b_fname in self.tables[1].indexes:
                        return (a_fname,b_fname)
                    elif self.names[0] == b_tname and a_fname in self.tables[1].indexes:
                        return (b_fname,a_fname)
        return None
        
    def __call__(self,i,j=None) :
        """
        Given a row index (or pair of row indices for a join) produces a boolean condition result.
        Input:
        i - row index
        j - second row index (for joins)
        Returns:
        boolean condition result
        """
        bools = [f(i,j) for f in self.funs]
        return all(bools) if self.isAnd else any(bools)
