<?php
namespace MyQEE\Database;

/**
 * 数据库SQL类驱动扩展类
 *
 * @author     呼吸二氧化碳 <jonwang@myqee.com>
 * @category   Database
 * @copyright  Copyright (c) 2008-2018 myqee.com
 * @license    http://www.myqee.com/license.html
 */
abstract class DriverSQL extends Driver
{
    /**
     * 引擎是MySQL
     *
     * @var bool
     */
    protected $isMySQL = false;

    /**
     * 返回查询类型
     *
     * ```php
     * list($sqlType, $needMaster)  = $this->getQueryType($sql);
     * ```
     *
     * @param $sql
     * @return array 第1个是$sqlType, 第2个是 是否需要使用主数据库
     */
    protected function getQueryType($sql)
    {
        if (preg_match('#^([a-z]+)(:? |\n|\r)#i', $sql, $m))
        {
            $type = strtoupper($m[1]);
        }
        else
        {
            $type = null;
        }

        $typeArr = [
            'SELECT',
            'SHOW',     //显示表
            'EXPLAIN',  //分析
            'DESCRIBE', //显示结结构
            'INSERT',
            'REPLACE',
            'UPDATE',
            'DELETE',
        ];

        if (!in_array($type, $typeArr))
        {
            $type = 'MASTER';
        }

        $slaveType = ['SELECT', 'SHOW', 'EXPLAIN'];

        if ($type !== 'MASTER' && in_array($type, $slaveType))
        {
            $needMaster = false;
        }
        else
        {
            $needMaster = true;
        }

        return [$type, $needMaster];
    }

    /**
     * 创建一个数据库
     *
     * @param string $database
     * @param string $charset 编码，不传则使用数据库连接配置相同到编码
     * @param string $collate 整理格式
     * @return boolean
     * @throws \Exception
     */
    public function createDatabase($database, $charset = null, $collate = null)
    {
        $config = $this->config;
        $this->config['connection']['database'] = null;

        if (!$charset)
        {
            $charset = $this->config['charset'];
        }
        $sql = 'CREATE DATABASE ' . $this->quoteIdentifier($database) .' DEFAULT CHARACTER SET '. $charset;

        if ($collate)
        {
            $sql .= ' COLLATE '. $collate;
        }
        try
        {
            $result       = $this->query($sql, null, true)->result();
            $this->config = $config;

            return $result;
        }
        catch (\Exception $e)
        {
            $this->config = $config;
            throw $e;
        }
    }

    /**
     * Quote a database table name and adds the table prefix if needed.
     *
     * $table = $db->quoteTable($table);
     *
     * @param   mixed  $value table name or array(table, alias)
     * @param   bool  $autoAsTable
     * @return  string
     * @uses    DB::quoteIdentifier
     * @uses    DB::tablePrefix
     */
    public function quoteTable($value, $autoAsTable = false)
    {
        if (is_array($value))
        {
            $table = & $value[0];
        }
        else
        {
            $table = & $value;
        }

        if ($this->config['prefix'] && is_string($table) && strpos($table, '.') === false)
        {
            if (stripos($table, ' AS ') !== false)
            {
                $table = $this->config['prefix'] . $table;
            }
            else
            {
                $table = $this->config['prefix'] . $table . ($autoAsTable ? ' AS ' . $table : '');
            }
        }

        return $this->quoteIdentifier($value);
    }

    /**
     * Quote a value for an SQL query.
     *
     * $db->quote(null);   // NULL
     * $db->quote(10);     // 10
     * $db->quote('fred'); // 'fred'
     *
     * @param   mixed  $value any value to quote
     * @return  string
     * @uses    Database::escape
     */
    public function quote($value)
    {
        if (null === $value)
        {
            return 'NULL';
        }
        elseif (true === $value)
        {
            return "'1'";
        }
        elseif (false === $value)
        {
            return "'0'";
        }
        elseif (is_object($value))
        {
            if ($value instanceof DB)
            {
                # 创建一个子查询SQL
                return '('. $value->compile() .')';
            }
            elseif ($value instanceof Expression)
            {
                # 使用一个不被解析的数据
                return $value->value();
            }
            elseif ($value instanceof \ArrayObject || $value instanceof \ArrayIterator || $value instanceof \stdClass)
            {
                return '('. implode(', ', array_map(array($this, __FUNCTION__), (array)$value)) .')';
            }
            else
            {
                # 转换成字符串
                return $this->quote((string)$value);
            }
        }
        elseif (is_array($value))
        {
            return '(' . implode(', ', array_map(array($this, __FUNCTION__), $value)) . ')';
        }
        elseif (is_int($value))
        {
            return "'". $value ."'";
        }
        elseif (is_float($value))
        {
            // Convert to non-locale aware float to prevent possible commas
            return sprintf('%F', $value);
        }

        return $this->escape($value);
    }

    /**
     * 构建SQL语句
     *
     * @param array $builder
     * @param string $type 支持 select (默认), insert, replace, insertUpdate, update, delete
     * @return string
     */
    public function compile($builder, $type = 'select')
    {
        switch ($type)
        {
            case 'insert':
                return $this->compileInsert($builder);

            case'replace':
                return $this->compileInsert($builder, 'REPLACE');

            case'insertUpdate':
                return $this->compileInsert($builder, 'REPLACE', true);

            case 'update':
                return $this->compileUpdate($builder);

            case 'delete':
                return $this->compileDelete($builder);

            default:
                return $this->compileSelect($builder);
        }
    }

    protected function quoteIdentifier($column)
    {
        if (is_array($column))
        {
            list($column, $alias) = $column;
        }

        if (is_object($column))
        {
            if ($column instanceof DB)
            {
                // Create a sub-query
                $column = '('. $column->compile() .')';
            }
            elseif ($column instanceof Expression)
            {
                // Use a raw expression
                $column = $column->value();
            }
            else
            {
                // Convert the object to a string
                $column = $this->quoteIdentifier((string)$column);
            }
        }
        else
        {
            # 转换为字符串
            $column = trim((string)$column);

            if (preg_match('#^(.*) AS (.*)$#i', $column, $m))
            {
                $column = $m[1];
                $alias  = $m[2];
            }

            if ($column === '*')
            {
                return $column;
            }
            elseif (strpos($column, '"') !== false)
            {
                // Quote the column in FUNC("column") identifiers
                $column = preg_replace('/"(.+?)"/e', '$this->quoteIdentifier("$1")', $column);
            }
            elseif (strpos($column, '.') !== false)
            {
                $parts = explode('.', $column);

                $prefix = $this->config['prefix'];
                if ($prefix)
                {
                    // Get the offset of the table name, 2nd-to-last part
                    $offset = count($parts) - 2;

                    if (!$this->asTable || !in_array($parts[$offset], $this->asTable))
                    {
                        $parts[$offset] = $prefix . $parts[$offset];
                    }
                }

                foreach ($parts as & $part)
                {
                    if ($part !== '*')
                    {
                        // Quote each of the parts
                        $this->convertEncoding($part);
                        $part = $this->identifier . str_replace([$this->identifier, '\\'], '', $part) . $this->identifier;
                    }
                }

                $column = implode('.', $parts);
            }
            else
            {
                $this->convertEncoding($column);
                $column = $this->identifier . str_replace([$this->identifier, '\\'], '', $column) . $this->identifier;
            }
        }

        if (isset($alias))
        {
            $this->convertEncoding($alias);
            $column .= ' AS ' . $this->identifier . str_replace([$this->identifier, '\\'], '', $alias) . $this->identifier;
        }

        return $column;
    }

    protected function compileSelect($builder)
    {
        $quoteIdentifier = [$this, 'quoteIdentifier'];

        $quoteTable = [$this, 'quoteTable'];

        $query = 'SELECT ';

        if ($builder['distinct'])
        {
            if (true === $builder['distinct'])
            {
                $query .= 'DISTINCT ';
            }
            else
            {
                $builder['selectAdv'][] = [
                    $builder['distinct'],
                    'distinct',
                ];
            }
        }

        $this->initAsTable($builder);
        $this->formatSelectAdv($builder);
        $this->formatGroupConcat($builder);

        if (empty($builder['select']))
        {
            $query .= '*';
        }
        else
        {
            $query .= implode(', ', array_unique(array_map($quoteIdentifier, $builder['select'])));
        }

        if (!empty($builder['from']))
        {
            // Set tables to select from
            $query .= ' FROM ' . implode(', ', array_unique(array_map($quoteTable, $builder['from'], array(true))));
        }

        if (!empty($builder['index']))
        {
            foreach ($builder['index'] as $item)
            {
                $query .= ' '. strtoupper($item[1]) .' INDEX('. $this->quoteIdentifier($item[0]) .')';
            }
        }

        if (!empty($builder['join']))
        {
            // Add tables to join
            $query .= ' '. $this->compileJoin($builder['join']);
        }

        if (!empty($builder['where']))
        {
            // Add selection conditions
            $query .= ' WHERE '. $this->compileConditions($builder['where']);
        }

        if (!empty($builder['groupBy']))
        {
            // Add sorting
            $query .= ' GROUP BY '. implode(', ', array_map($quoteIdentifier, $builder['groupBy']));
        }

        if (!empty($buœilder['having']))
        {
            // Add filtering conditions
            $query .= ' HAVING '. $this->compileConditions($builder['having']);
        }

        if (!empty($builder['orderBy']))
        {
            // Add sorting
            $query .= ' '. $this->compileOrderBy($builder['orderBy']);
        }

        if ($builder['limit'] !== null)
        {
            // Add limiting
            $query .= ' LIMIT '. $builder['limit'];
        }

        if ($builder['offset'] !== null)
        {
            // Add offsets
            $query .= ' OFFSET '. $builder['offset'];
        }

        return $query;
    }

    /**
     * 构造一条替换的语句
     *
     * @param $builder
     * @param string $type
     * @param bool $insertUpdate
     * @return string
     */
    protected function compileInsert($builder, $type = 'INSERT', $insertUpdate = false)
    {
        if ($this->isMySQL && $insertUpdate)
        {
            $typeString = 'INSERT';
        }
        else if ($type !== 'REPLACE')
        {
            $typeString = 'INSERT';
        }
        else
        {
            $typeString = $type;
        }

        $query = $typeString . ' INTO ' . $this->quoteTable($builder['table'], false);

        // Add the column names
        $query .= ' (' . implode(', ', array_map([$this, 'quoteIdentifier'], $builder['columns'])) .') ';

        if (is_array($builder['values']))
        {
            $quote  = [$this, 'quote'];
            $groups = [];

            foreach ($builder['values'] as $group)
            {
                $groups[] = '('. implode(', ', array_map($quote, $group)) .')';
            }

            if (count($groups) > 1)
            {
                $query .= "\nVALUES\n". implode(",\n", $groups);
            }
            else
            {
                $query .= 'VALUES '. implode(",\n", $groups);
            }
        }
        else
        {
            // Add the sub-query
            $query .= (string)$builder['values'];
        }

        if ($type === 'REPLACE')
        {
            //where
            if (!empty($builder['where']))
            {
                // Add selection conditions
                $query .= ' WHERE '. $this->compileConditions($builder['where']);
            }

            if ($this->isMySQL && $insertUpdate)
            {
                $query .= ' '. $this->compileOnDuplicateKeyUpdate($builder);
            }
        }

        return $query;
    }

    /**
     * 构造 `ON DUPLICATE KEY UPDATE ...` 语句
     *
     * @param $builder
     * @return string
     */
    protected function compileOnDuplicateKeyUpdate($builder)
    {
        $query = 'ON DUPLICATE KEY UPDATE ';

        $groups = array();
        foreach ($builder['columns'] as $column)
        {
            $c = $this->quoteIdentifier($column);

            $groups[] = "{$c} = VALUES({$c})";
        }

        return $query . implode(', ', $groups);
    }

    protected function compileUpdate($builder)
    {
        // Start an update query
        $query = 'UPDATE '. $this->quoteTable($builder['table'], false);

        // Add the columns to update
        $query .= ' SET '. $this->compileSet($builder['set']);

        if (!empty($builder['where']))
        {
            // Add selection conditions
            $query .= ' WHERE '. $this->compileConditions($builder['where']);
        }

        if (!empty($builder['orderBy']))
        {
            // Add sorting
            $query .= ' '. $this->compileOrderBy($builder['orderBy']);
        }

        if ($builder['limit'] !== null)
        {
            // Add limiting
            $query .= ' LIMIT '. $builder['limit'];
        }

        if ($builder['offset'] !== null)
        {
            // Add offsets
            $query .= ' OFFSET '. $builder['offset'];
        }

        return $query;
    }

    protected function compileDelete($builder)
    {
        // Start an update query
        $query = 'DELETE FROM'. $this->quoteTable($builder['table'], false);

        if (!empty($builder['where']))
        {
            $this->initAsTable($builder);

            // Add selection conditions
            $query .= ' WHERE '. $this->compileConditions($builder['where']);
        }

        return $query;
    }

    /**
     * Compiles an array of ORDER BY statements into an SQL partial.
     *
     * @param   array  $columns sorting columns
     * @return  string
     */
    protected function compileOrderBy(array $columns)
    {
        $sort = [];

        foreach ($columns as $group)
        {
            list ($column, $direction) = $group;

            if (is_string($direction))
            {
                $sort[] = $this->quoteIdentifier($column) .' '. $direction;
            }
            elseif (is_array($direction))
            {
                foreach ($direction as &$d)
                {
                    $d = $this->quote($d);
                }
                // ORDER BY FIELD(`test`, 1, 3, 2, 8, 5);
                $sort[] = 'FIELD('. $this->quoteIdentifier($column) .', '. implode(', ', $direction) .')';
            }
        }

        return 'ORDER BY '. implode(', ', $sort);
    }

    /**
     * Compiles an array of conditions into an SQL partial. Used for WHERE
     * and HAVING.
     *
     * @param   array  $conditions condition statements
     * @return  string
     */
    protected function compileConditions(array $conditions)
    {
        $lastCondition = null;

        $sql = '';
        foreach ($conditions as $group)
        {
            // Process groups of conditions
            foreach ($group as $logic => $condition)
            {
                if ($condition === '(')
                {
                    if (!empty($sql) && $lastCondition !== '(')
                    {
                        // Include logic operator
                        $sql .= ' ' . $logic . ' ';
                    }

                    $sql .= '(';
                }
                elseif ($condition === ')')
                {
                    $sql .= ')';
                }
                else
                {
                    if (!empty($sql) && $lastCondition !== '(')
                    {
                        // Add the logic operator
                        $sql .= ' '. $logic .' ';
                    }

                    // Split the condition
                    list ($column, $op, $value) = $condition;

                    if ($value === null)
                    {
                        if ($op === '=')
                        {
                            // Convert "val = NULL" to "val IS NULL"
                            $op = 'IS';
                        }
                        elseif ($op === '!=' || $op === '<>')
                        {
                            // Convert "val != NULL" to "valu IS NOT NULL"
                            $op = 'IS NOT';
                        }
                    }

                    // Database operators are always uppercase
                    $op = strtoupper($op);

                    if (is_array($value) && count($value) <= 1)
                    {
                        # 将in条件下只有1条数据的改为where方式
                        if ($op === 'IN')
                        {
                            $op = '=';
                            $value = current($value);
                        }
                        elseif ($op === 'NOT IN')
                        {
                            $op = '!=';
                            $value = current($value);
                        }
                    }

                    if ($op === 'BETWEEN' && is_array($value))
                    {
                        // BETWEEN always has exactly two arguments
                        list ($min, $max) = $value;

                        // Quote the min and max value
                        $value = $this->quote($min) .' AND '. $this->quote($max);
                    }
                    elseif ($op === 'MOD')
                    {
                        $value = $this->quote($value[0]) .' '. strtoupper($value[2]) .' '. $this->quote($value[1]);
                    }
                    else
                    {
                        if (is_array($value))
                        {
                            if ($op === '=')
                            {
                                $op = 'IN';
                            }
                            elseif ($op === '!=')
                            {
                                $op = 'NOT IN';
                            }
                        }

                        $value = $this->quote($value);
                    }

                    // Append the statement to the query
                    $sql .= $this->quoteIdentifier($column) .' '. $op .' '. $value;
                }

                $lastCondition = $condition;
            }
        }

        return $sql;
    }

    /**
     * Compiles an array of JOIN statements into an SQL partial.
     *
     * @param   array  $joins join statements
     * @return  string
     */
    protected function compileJoin(array $joins)
    {
        $statements = [];

        foreach ($joins as $join)
        {
            $statements[] = $this->compileJoinOn($join);
        }

        return implode(' ', $statements);
    }

    protected function compileJoinOn($join)
    {
        if ($join['type'])
        {
            $sql = strtoupper($join['type']) .' JOIN';
        }
        else
        {
            $sql = 'JOIN';
        }

        // Quote the table name that is being joined
        $sql .= ' '. $this->quoteTable($join['table'], true) .' ON ';

        $conditions = array();
        foreach ($join['on'] as $condition)
        {
            // Split the condition
            list ($c1, $op, $c2) = $condition;

            if ($op)
            {
                // Make the operator uppercase and spaced
                $op = ' ' . strtoupper($op);
            }

            // Quote each of the identifiers used for the condition
            $conditions[] = $this->quoteIdentifier($c1) . $op .' '. $this->quoteIdentifier($c2);
        }

        // Concat the conditions "... AND ..."
        $sql .= '('. implode(' AND ', $conditions) .')';

        return $sql;
    }

    /**
     * Compiles an array of set values into an SQL partial. Used for UPDATE.
     *
     * @param   array  $values updated values
     * @return  string
     */
    protected function compileSet(array $values)
    {
        $set = array();
        foreach ($values as $group)
        {
            // Split the set
            list ($column, $value , $op) = $group;

            if ($op === '+' || $op === '-')
            {
                $type = $op;
            }
            else
            {
                $type = '';
            }

            $column = $this->quoteIdentifier($column);

            if ($type)
            {
                $set[$column] = $column .' = '. $column .' '. $type .' '. $this->quote($value);
            }
            else
            {
                $set[$column] = $column .' = '. $this->quote($value);
            }
        }

        return implode(', ', $set);
    }


    /**
     * 初始化所有的 asTable
     */
    protected function initAsTable($builder)
    {
        $this->asTable = [];

        if ($builder['from'])
        {
            foreach ($builder['from'] as $item)
            {
                $this->doInitAsTable($item);
            }
        }

        if ($builder['join'])
        {
            foreach ($builder['join'] as $item)
            {
                $this->doInitAsTable($item['table']);
            }
        }
    }

    protected function doInitAsTable($value)
    {
        if (is_array($value))
        {
            list ($value) = $value;
        }
        elseif (is_object($value))
        {
            if ($value instanceof DB)
            {
                $value = $value->compile();
            }
            elseif ($value instanceof Expression)
            {
                $value = $value->value();
            }
            else
            {
                $value = (string)$value;
            }
        }
        $value = trim($value);

        if (preg_match('#^(.*) AS ([a-z0-9`_]+)$#i', $value , $m))
        {
            $alias = $m[2];
        }
        elseif ($this->config['prefix'] && strpos($value, '.') === false)
        {
            $alias = $value;
        }
        else
        {
            $alias = null;
        }

        if ($alias)
        {
            $this->asTable[] = $alias;
        }
    }

    /**
     * 格式化高级查询参数到select里
     */
    protected function formatSelectAdv(& $builder)
    {
        if ($builder['selectAdv'])foreach ($builder['selectAdv'] as $item)
        {
            if (!is_array($item))continue;

            if (is_array($item[0]))
            {
                $column = $item[0][0];
                $alias  = $item[0][1];
            }
            else if (preg_match('#^(.*) AS (.*)$#i', $item[0], $m))
            {
                $column = $this->quoteIdentifier($m[1]);
                $alias  = $m[2];
            }
            else
            {
                $column = $this->quoteIdentifier($item[0]);
                $alias = $item[0];
            }

            // 其它参数
            $argsStr = '';
            if (($countItem = count($item)) > 2)
            {
                for($i=2; $i < $countItem; $i++)
                {
                    $argsStr .= ','. $this->quoteIdentifier($item[$i]);
                }
            }

            $builder['select'][] = [
                DB::exprValue(strtoupper($item[1]) .'('. $this->quoteIdentifier($column.$argsStr) .')'),
                $alias,
            ];
        }
    }

    /**
     * 解析 GROUP_CONCAT
     *
     * @param array $arr
     */
    protected function formatGroupConcat(& $builder)
    {
        if ($builder['groupConcat'])foreach($builder['groupConcat'] as $item)
        {
            if (is_array($item[0]))
            {
                $column = $item[0][0];
                $alias  = $item[0][1];
            }
            else if (preg_match('#^(.*) AS (.*)$#i', $item[0] , $m))
            {
                $column = $this->quoteIdentifier($m[1]);
                $alias  = $m[2];
            }
            else
            {
                $column = $this->quoteIdentifier($item[0]);
                $alias  = $item[0];
            }

            $str = 'GROUP_CONCAT(';

            if (isset($item[3]) && $item[3])
            {
                $str .= 'DISTINCT ';
            }
            $str .= $column;

            if (isset($item[1]) && $item[1])
            {
                $str .= ' ORDER BY '. $column .' '. (strtoupper($item[1]) === 'DESC' ? 'DESC' : 'ASC');
            }

            if ($item[2])
            {
                $str .= ' SEPARATOR '. $this->quoteIdentifier($item[2]);
            }

            $str .= ')';

            $builder['select'][] = [
                DB::exprValue($str),
                $alias,
            ];
        }
    }
}