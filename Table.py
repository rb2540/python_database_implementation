from BTrees.OOBTree import OOBTree

def format_data(s) :
    try :
        return int(s)
    except ValueError :
        return s

def format_str(s) :
    try :
        v = int(s)
        return str(s)
    except ValueError :
        pass
    try :
        v = float(s)
        return f'{v:0.4f}'
    except ValueError :
        return str(s)
class Table :
    def __init__(self, filename=None) :
        """
        Constructs a Table object using data from the given filename.
        Input:
        filename - filename of a file containing the |-delimited data
        """
        self.reset()
        if filename is not None :
            self.load(filename)

    def __str__(self) :
        s = '|'.join(self.names)+'\n'
        for i in range(len(self)) :
            d = [str(self.columns[j][i]) for j in range(len(self.columns))]
            s += '|'.join(d)+'\n'
        return s

    def __repr__(self) : return str(self)
        
            
    @classmethod
    def create_table(cls, names) :
        """
        Creates an empty table with a given list of names
        Input:
        names - column names for table
        Returns:
        Table with given column names
        """
        C = cls()
        C.names = names.copy()
        C.columns = [[] for n in C.names]
        C.namemap = {C.names[i]:C.columns[i] for i in range(len(C.names))}
        return C
            
    def reset(self) :
        """
        Resets the state of a table.
        """
        self.indexes = dict()
        self.names = []
        self.columns = []
        self.namemap = dict()

    def add_row(self,vdict) :
        for k,c in self.namemap.items() :
            c.append(vdict[k]) #throws exception if value missing

    def load(self, filename) :
        """
        Loads data from the given filename.
        Input:
        filename - filename of a file containing the |-delimited data
        """
        with open(filename) as f :
            lines = f.readlines()
        self.names = [s.strip() for s in lines[0].split('|')]
        self.columns = [[] for i in self.names]
        self.namemap = {self.names[i]:self.columns[i] for i in range(len(self.names))}
        for i in range(1,len(lines)) :
            vals = [format_data(s.strip()) for s in lines[i].split('|')]
            for j in range(len(self.columns)) :
                self.columns[j].append(vals[j]) #throws exception on misformatted data
        print(f'Loaded {len(self)} rows from {filename}')
                
    def write(self, filename) :
        """
        Writes data to file in |-delimited format.
        Input:
        filename - filename of file to write data to
        """
        with open(filename,'w') as f :
            f.write('|'.join(self.names)+'\n') #do not write out column names
            for i in range(len(self)) :
                d = [format_str(self.columns[j][i]) for j in range(len(self.columns))]
                f.write('|'.join(d)+'\n')
                
    def __len__(self) :
        """
        Returns the number of rows in the table.
        """
        return len(self.columns[0]) if len(self.columns) > 0 else 0

    def get_row(self,i) :
        """
        Returns the ith row as a dictionary.
        Input:
        i - row number
        Outputs:
        dictionary mapping column name to values in row
        """
        return {n:c[i] for n,c in self.namemap.items()}

    def hash(self,name) :
        d = dict()
        col = self.namemap[name]
        for i in range(len(self)) :
            L = d.get(col[i],[])
            L.append(i)
            d[col[i]] = L
        self.indexes[name] = d

    def btree(self,name) :
        d = OOBTree()
        col = self.namemap[name]
        for i in range(len(self)) :
            L = d.get(col[i],[])
            L.append(i)
            d[col[i]] = L
        self.indexes[name] = d

    def select(self,condition) :
        """
        Returns a new table obtained by selecting rows from 
        this table according to the given condition.
        Input:
        condition - Condition on which rows to select
        Output:
        new table whose rows are determined by the given condition
        """
        res = Table.create_table(self.names)
        idxs = range(len(self))
        idx_info = condition.get_select_index()
        if idx_info is not None:
            idxs = self.indexes[idx_info[0]].get(idx_info[1],[])
        for i in idxs :
            if condition(i) :
                res.add_row(self.get_row(i))
        return res

    def project(self,col_list) :
        """
        Returns a new table obtained by selecting columns from 
        this table.
        Input:
        col_list - list of column names to select
        Output:
        new table whose columns are determined by the given column list
        """
        res = Table.create_table(col_list)
        for i in range(len(self)) :
            vdict = self.get_row(i)
            res.add_row(vdict)
        return res

    def join(self,T,p1,p2,condition) :
        """
        Returns a new table obtained by joining this table and table T.
        The new columns have prefixes p1 (this table) and p2 (table T).
        Which rows to include are determined by the given condition.
        Input:
        T - table to join with
        p1 - prefix for this table
        p2 - prefix for table T
        condition - Condition determining which rows to include
        Output:
        new table obtained by joining this table with T according to condition
        """
        names = [p1+'_'+n for n in self.names]+[p2+'_'+n for n in T.names]
        res = Table.create_table(names)
        idx_info = condition.get_join_index()
        if idx_info is not None :
            colA = self.namemap[idx_info[0]]
            idx = T.indexes[idx_info[1]]            
        for i in range(len(self)) :
            L = range(len(T))
            if idx_info is not None : L = idx.get(colA[i],[])
            for j in L :
                if condition(i,j) :
                    vdict = {p1+'_'+n:c[i] for n,c in self.namemap.items()}
                    vdict.update({p2+'_'+n:c[j] for n,c in T.namemap.items()})
                    res.add_row(vdict)
        return res

    def concat(self,T) :
        """
        Returns a new table obtained by adding the rows of T to this table.
        Tables must have the same schema.
        Input:
        T - other table to obtain rows from
        Output:
        new table containing data from table T added to this table
        """
        res = Table.create_table(self.names)
        for i in range(len(self)) :
            res.add_row(self.get_row(i))            
        for i in range(len(T)) :
            res.add_row(T.get_row(i))
        return res
            
    def sort(self,col_names) :
        """
        Returns a new table that contains the rows of this table sorted
        according to col_names, with earlier names having higher priority.
        Input:
        col_names - column names to sort by, in order of priority
        Output:
        new sorted table
        """
        res = Table.create_table(self.names)
        if len(res) == 0 : return res
        for i in range(len(self)) :
            res.add_row(self.get_row(i))            
        for n in col_names[::-1] :
            argsort = sorted(range(len(res)),key=res.namemap[n].__getitem__)
            for i in range(len(res.columns)) :
                res.columns[i] = [res.columns[i][j] for j in argsort]
        return res

    def sumgroup(self,opcol_name,groupcol_names) :
        """
        Returns a new table by applying opgroup with sum. (see opgroup)
        """        
        return self.opgroup(opcol_name,groupcol_names,sum,'sum')

    def avggroup(self,opcol_name,groupcol_names) :
        """
        Returns a new table by applying opgroup with average. (see opgroup)
        """
        avg = lambda L : sum(L)/len(L) if len(L) > 0 else 0
        return self.opgroup(opcol_name,groupcol_names,avg,'avg')

    def countgroup(self,opcol_name,groupcol_names) :
        """
        Returns a new table by applying opgroup with count. (see opgroup)
        """
        count = lambda L : len(L)
        return self.opgroup(opcol_name,groupcol_names,count,'count')
    
    def opgroup(self,opcol_name,groupcol_names,op,op_name) :
        """
        Returns a new table where the values in column opcol_name
        are combined with operation op grouped by columns in groupcol_names.
        Input:
        opcol_name - name of column to combine
        groupcol_names - list of column names to groupby
        op - operator to combine with
        op_name - name of operator (prefix of column name)
        Output:
        new table with combined and grouped by columns
        """
        d = dict()
        op_col = self.namemap[opcol_name]
        group_cols = [self.namemap[g] for g in groupcol_names]
        for i in range(len(self)) :
            op_val = op_col[i]
            group_val = tuple(g[i] for g in group_cols)
            L = d.get(group_val,[])
            L.append(op_val)
            d[group_val] = L
        c_name = op_name+'_'+opcol_name
        res = Table.create_table([c_name]+groupcol_names)
        for k,L in d.items() :
            v = op(L)
            vdict = {c_name:v}
            kdict = {groupcol_names[i]:k[i] for i in range(len(k))}
            vdict.update(kdict)
            res.add_row(vdict)
        return res

    def sum(self,opcol_name) :
        """
        Returns a new table by applying optable with sum. (see optable)
        """        
        return self.optable(opcol_name,sum,'sum')

    def avg(self,opcol_name) :
        """
        Returns a new table by applying optable with avg. (see optable)
        """        
        avg = lambda L : sum(L)/len(L) if len(L) > 0 else 0
        return self.optable(opcol_name,avg,'avg')

    def count(self,opcol_name) :
        """
        Returns a new table by applying optable with count. (see optable)
        """        
        count = lambda L : len(L)
        return self.optable(opcol_name,count,'count')

    def optable(self,opcol_name,op,op_name) :
        """
        Returns a new table with a single column with 1 row containing
        op applied to the given column.
        Input:
        opcol_name - name of column to combine
        op - operator to combine with
        op_name - name of operator (prefix of column name)
        Output:
        new table with combined value
        """
        opcol = self.namemap[opcol_name]
        c_name = op_name+'_'+opcol_name
        res = Table.create_table([c_name])
        res.add_row({c_name:op(opcol)})
        return res
        
    def movsum(self,opcol_name,w) :
        """
        Returns a new table by applying movop with sum. (see movop)
        """        
        return self.movop(opcol_name,sum,w,'mov_sum')
    
    def movavg(self,opcol_name,w) :
        """
        Returns a new table by applying movop with avg. (see movop)
        """        
        avg = lambda L : sum(L)/len(L) if len(L) > 0 else 0
        return self.movop(opcol_name,avg,w,'mov_avg')

    def movop(self,opcol_name,op,w,res_name) :
        """
        Returns a new table with a single column appended included a moving
        window operation applied to opcol_name with window length w and operation op.
        Input:
        opcol_name - name of column to apply moving window to
        op - operator to use on window
        w - window length
        res_name - appended column name
        Output:
        new table with column appended
        """
        opcol = self.namemap[opcol_name]
        res = Table.create_table(self.names+[res_name])
        for i in range(len(self)) :
            vdict = self.get_row(i)
            vdict[res_name] = op(opcol[max(i-w+1,0):i+1])
            res.add_row(vdict)
        return res
        
        

    
