<?php
namespace MyQEE\Database;

/**
 * SQL构造核心类
 *
 * @author     呼吸二氧化碳 <jonwang@myqee.com>
 * @category   Database
 * @copyright  Copyright (c) 2008-2016 myqee.com
 * @license    http://www.myqee.com/license.html
 */
class QueryBuilder
{
    /**
     * Builder数组
     *
     * @var array
     */
    protected $_builder = array();

    /**
     * Builder备份
     *
     * @var array
     */
    protected $_builderBak = array();

    protected $_lastJoin = null;

    public function __construct()
    {
        # 初始化数据
        $this->reset();
    }

    /**
     * 设定一个预处理语句
     *
     * 此时只是设定一个预处理语句，并不执行，通过 `$this->execute()` 来执行
     * 设置的模板在 `$this->execute()` 后仍可反复使用，直到重新设置
     *
     * [!!] 本方法只能设置1条，再次执行则覆盖之前设置的，与 `PDD::prepare($statement)` 不同，它反复执行可以设置多条
     *
     *      $db = new Database();
     *
     *      // 用法1，替换掉相同关键字的部分
     *      $rp = array
     *      (
     *          ':id'     => $_GET['id'],
     *          ':status' => $_GET['status'],
     *      );
     *      $rs = $db->prepare("SELECT * FROM `my_table` WHERE id = :id AND status = :status")->execute($rp);
     *
     *      // 用法2，按顺序替换掉语句中?的部分
     *      $rp = array
     *      (
     *          $_GET['id'],
     *          $_GET['status'],
     *      );
     *      $rs = $db->prepare("SELECT * FROM `my_table` WHERE id = ? AND status = ?")->execute($rp);
     *
     * @param $statement
     * @return $this
     */
    public function prepare($statement)
    {
        $this->_builder['statement'] = trim($statement);

        return $this;
    }

    /**
     * 获取Builder配置
     *
     * 可用 $builder = & $obj->get_builder(); 获取到内存指针
     *
     * @return array
     */
    public function & getBuilder()
    {
        return $this->_builder;
    }

    /**
     * 设置Builder信息
     *
     *      // 设置一样的builder
     *      $db->from('mytable')->where('id>', 10)->limit(10)->order_by('id', 'DESC');
     *
     *      // 获取当前builder
     *      $builder = $db->get_builder();
     *
     *      // 执行查询
     *      $data1 = $db->where('type', 1)->get()->as_array();
     *      echo $db->last_query();     //SELECT * FROM `mytable` WHERE `id` > 10 AND `type` = '1' ORDER BY `id` DESC LIMIT 10
     *
     *      // 将前面获取的builder重新设置回去
     *      $db->set_builder($builder);
     *
     *      // 再次执行另外一个附加条件的查询
     *      $data2 = $db->where('type', 3)->get()->as_array();
     *      echo $db->last_query();     //SELECT * FROM `mytable` WHERE `id` > 10 AND `type` = '3' ORDER BY `id` DESC LIMIT 10
     *
     *
     * @param array $builder builder信息数组，不必完整的，建议通过get_builder()获取后设置
     * @return $this
     */
    public function setBuilder(array $builder)
    {
        $this->_builder = array_merge($this->_builder, $builder);

        return $this;
    }

    /**
     * 构成查询 SELECT DISTINCT
     * 如果传的是字符串则构造出 SELECT DISTINCT(`test`) as `test` 这样的查询(MySQL)
     *
     * @param  boolean $value enable or disable distinct columns
     * @return $this
     */
    public function distinct($value = true)
    {
        $this->_builder['distinct'] = $value;

        return $this;
    }

    /**
     * select(c1, c2, c3,......)
     *
     * 如果查询是SELECT * 则不需要设置，系统会自动处理
     *
     *      $db->select('id', 'username')->from('members')->get()->as_array();
     *      echo $db->last_query();     //SELECT `id`, `username` FROM `members`;
     *
     *      $db->select('db1.id', 'db2.username')->from('members as db1')->join('mydb as db2')->on('db1.id', 'db2.mid')->get()->as_array();
     *      echo $db->last_query();     //SELECT `db1`.`id`, `db2`.`username` FROM `members` AS `db1` JOIN ON `db1`.`id` = `db2`.`mid`;
     *
     *      // 使用Database::expr_value()方法可以传入一个不被解析的字符串
     *      $db->select(Database::expr_value('SUM("id") as `id`'))->from('members')->get()->as_array();
     *      echo $db->last_query();     //SELECT SUM("id") as `id` FROM `members`;
     *
     *
     * @param  mixed $columns column name or array($column, $alias) or object
     * @param  ...
     * @return $this
     */
    public function select($columns)
    {
        if (func_num_args() > 1)
        {
            $columns = func_get_args();
        }
        elseif (is_string($columns))
        {
            $columns = explode(',', $columns);
        }
        elseif (!is_array($columns))
        {
            $columns = array($columns);
        }

        $this->_builder['select'] = array_merge($this->_builder['select'], $columns);

        return $this;
    }

    /**
     * Choose the columns to select from, using an array.
     *
     * @param  array $columns list of column names or aliases
     * @return $this
     */
    public function select_array(array $columns)
    {
        $this->_builder['select'] = array_merge($this->_builder['select'], $columns);

        return $this;
    }

    /**
     * 查询最大值
     *
     *    $db->select_max('test')->from('db')->group_by('class_id')->get()->as_array();
     *
     * @param string $column
     * @return $this
     */
    public function selectMax($column)
    {
        $this->selectAdv($column, 'max');

        return $this;
    }

    /**
     * 查询平均值
     *
     *    $db->select_min('test')->from('db')->group_by('class_id')->get()->as_array();
     *
     * @param string $column
     * @return $this
     */
    public function selectMin($column)
    {
        $this->selectAdv($column, 'min');

        return $this;
    }

    /**
     * 查询平均值
     *
     *    $db->select_avg('test')->from('db')->group_by('class_id')->get()->as_array();
     *
     * @param string $column
     * @return $this
     */
    public function selectAvg($column)
    {
        $this->selectAdv($column, 'avg');

        return $this;
    }

    /**
     * 查询总和
     *
     *    $db->select_sum('test')->from('db')->group_by('class_id')->get()->as_array();
     *
     * @param string $column
     * @return $this
     */
    public function selectSum($column)
    {
        $this->selectAdv($column, 'sum');

        return $this;
    }

    /**
     * 高级查询方式
     *
     * 需要相应接口支持，
     * 目前支持MongoDB的aggregation框架Group查询：$sum,$max,$min,$avg,$last,$first等，详情见 http://docs.mongodb.org/manual/reference/aggregation/group/
     * MySQL支持sum,max,min,svg等
     *
     *    $db->select_adv('test','max');        //查询最大值
     *    $db->select_adv('test','sum',3);      //查询+3的总和
     *
     * @param string $column
     * @param string $opt
     * @return $this
     */
    public function selectAdv($column, $type, $opt1 = null, $opt2 = null)
    {
        $this->_builder['select_adv'][] = func_get_args();

        return $this;
    }

    /**
     * Set the columns that will be inserted.
     *
     * @param  array $columns column names
     * @return $this
     */
    public function columns(array $columns)
    {
        $this->_builder['columns'] = $columns;

        return $this;
    }

    /**
     * 加入多条数据
     *
     *     // 例1
     *     $v1 = array('k1'=>1,'k2'=>1);
     *     $v2 = array('k1'=>2,'k2'=>1);
     *     $v3 = array('k1'=>3,'k2'=>1);
     *     $db->values($v1,$v2,$v3);        //加入3行数据
     *
     *     // 例2
     *     $values = array();
     *     $values[] = array('k1'=>1,'k2'=>1);
     *     $values[] = array('k1'=>2,'k2'=>1);
     *     $values[] = array('k1'=>3,'k2'=>1);
     *     $db->values($values);            //加入3行数据,等同上面的效果
     *
     * @param  array $values values list
     * @param  ...
     * @return $this
     */
    public function values(array $values)
    {
        if (is_array($values) && isset($values[0]) && is_array($values[0]))
        {
            // 多行数据
            // $values = $values;
        }
        else
        {
            $values = func_get_args();
        }

        $this->_builder['values'] = array_merge($this->_builder['values'], $values);

        return $this;
    }

    /**
     * 为update,insert设置数据
     *
     * @param  array $pairs associative (column => value) list
     * @return $this
     */
    public function set(array $pairs)
    {
        foreach ($pairs as $column => $value)
        {
            $column = trim($column);

            if (preg_match('#^(.*)(\+|\-)$#', $column, $m))
            {
                $column = $m[1];
                $op     = $m[2];
            }
            else
            {
                $op = '=';
            }
            $this->_builder['set'][] = [$column, $value, $op];
        }

        return $this;
    }

    /**
     * Set the value of a single column.
     *
     * @param  mixed $column table name or array($table, $alias) or object
     * @param  mixed $value column value
     * @param  string =|+|-
     * @return $this
     */
    public function value($column, $value, $op = '=')
    {
        $this->_builder['set'][] = [$column, $value, $op];

        return $this;
    }

    /**
     * 数据递增
     *
     * @param string $column
     * @param int $value
     * @return $this
     */
    public function valueIncrement($column, $value)
    {
        return $this->value($column, abs($value), $value > 0 ? '+' : '-');
    }

    /**
     * 数据递减
     *
     * @param string $column
     * @param int $value
     * @return $this
     */
    public function valueDecrement($column, $value)
    {
        return $this->valueIncrement($column, -$value);
    }

    /**
     * Sets the table to update.
     *
     * @param mixed $table table name or array($table, $alias) or object
     * @return $this
     */
    public function table($table)
    {
        $this->_builder['table'] = $table;

        return $this;
    }

    /**
     * from(tableA, tableB,...)
     *
     *      $db->from('mytable');                        // FROM `mytable`
     *      $db->from('mytable as tb1');                 // FROM `mytable` AS `tb1`
     *      $db->from('tb1', 'tb2');                     // FROM `tb1`, `tb2`
     *      $db->from('tb1', array('table2', 'tb2'));    // FROM `tb1`, `table2` AS `tb2`
     *      $db->from(array('table1', 'tb1'));           // FROM `table1` AS `tb1`
     *
     * @param  mixed $tables table name or array($table, $alias) or object
     * @param  ...
     * @return $this
     */
    public function from($tables)
    {
        if (func_num_args() > 1)
        {
            $tables = func_get_args();
        }
        elseif (is_string($tables))
        {
            $tables = explode(',', $tables);
        }
        elseif (is_array($tables))
        {
            $tables = array($tables);
        }

        $this->_builder['from'] = array_merge($this->_builder['from'], $tables);

        return $this;
    }

    /**
     * Adds addition tables to "JOIN ...".
     *
     * @param  mixed $table column name or array($column, $alias) or object
     * @param  string $type join type (LEFT, RIGHT, INNER, etc)
     * @return $this
     */
    public function join($table, $type = null)
    {
        $this->_builder['join'][] = [
            'table' => $table,
            'type'  => $type,
            'on'    => [],
        ];
        end($this->_builder['join']);
        $k = key($this->_builder['join']);
        unset($this->_lastJoin);
        $this->_lastJoin = &$this->_builder['join'][$k];

        return $this;
    }

    /**
     * Adds "ON ..." conditions for the last created JOIN statement.
     *
     * @param  mixed $c1 column name or array($column, $alias) or object
     * @param  string $c2 logic operator
     * @param  mixed $op column name or array($column, $alias) or object
     * @return $this
     */
    public function on($c1, $c2, $op = '=')
    {
        $this->_lastJoin['on'][] = [$c1, $op, $c2];

        return $this;
    }

    /**
     * group_by(c1,c2,c3,.....)
     *
     * @param  mixed $columns column name or array($column, $alias) or object
     * @param  ...
     * @return $this
     */
    public function groupBy($columns)
    {
        $columns = func_get_args();

        $this->_builder['group_by'] = array_merge($this->_builder['group_by'], $columns);

        return $this;
    }

    /**
     * 构成生成 GROUP_CONCAT() 的语句
     *
     * @param $column
     * @param string $order_by
     * @param string $separator
     * @param bool $distinct
     * @return $this
     */
    public function groupConcat($column, $order_by = null, $separator = null, $distinct = false)
    {
        $this->_builder['group_concat'][] = func_get_args();

        return $this;
    }

    /**
     * Alias of and_having()
     *
     * @param  mixed $column column name or array($column, $alias) or object
     * @param  string $value logic operator
     * @param  mixed $op column value
     * @return $this
     */
    public function having($column, $value = null, $op = '=')
    {
        if (is_array($column))
        {
            foreach ($column as $c => $value)
            {
                $this->andHaving($c, $value, $op);
            }
            return $this;
        }

        return $this->andHaving($column, $value, $op);
    }

    /**
     * Creates a new "AND HAVING" condition for the query.
     *
     * @param  mixed $column column name or array($column, $alias) or object
     * @param  string $value logic operator
     * @param  mixed $op column value
     * @return $this
     */
    public function andHaving($column, $value = null, $op = '=')
    {
        $this->_builder['having'][] = ['AND' => [$column, $op, $value]];

        return $this;
    }

    /**
     * Creates a new "OR HAVING" condition for the query.
     *
     * @param  mixed $column column name or array($column, $alias) or object
     * @param  string $value logic operator
     * @param  mixed $op column value
     * @return $this
     */
    public function orHaving($column, $value = null, $op = '=')
    {
        $this->_builder['having'][] = ['OR' => [$column, $op, $value]];

        return $this;
    }

    /**
     * Alias of and_having_open()
     *
     * @return $this
     */
    public function havingOpen()
    {
        return $this->andHavingOpen();
    }

    /**
     * Opens a new "AND HAVING (...)" grouping.
     *
     * @return $this
     */
    public function andHavingOpen()
    {
        $this->_builder['having'][] = ['AND' => '('];

        return $this;
    }

    /**
     * Opens a new "OR HAVING (...)" grouping.
     *
     * @return $this
     */
    public function orHavingOpen()
    {
        $this->_builder['having'][] = ['OR' => '('];

        return $this;
    }

    /**
     * Closes an open "AND HAVING (...)" grouping.
     *
     * @return $this
     */
    public function havingClose()
    {
        return $this->andHavingClose();
    }

    /**
     * Closes an open "AND HAVING (...)" grouping.
     *
     * @return $this
     */
    public function andHavingClose()
    {
        $this->_builder['having'][] = ['AND' => ')'];

        return $this;
    }

    /**
     * Closes an open "OR HAVING (...)" grouping.
     *
     * @return $this
     */
    public function orHavingClose()
    {
        $this->_builder['having'][] = ['OR' => ')'];

        return $this;
    }

    /**
     * Start returning results after "OFFSET ..."
     *
     * @param  integer $number starting result number
     * @return $this
     */
    public function offset($number)
    {
        $number = (int)$number;
        if ($number > 0)
        {
            $this->_builder['offset'] = $number;
        }

        return $this;
    }

    /**
     * 重设数据
     *
     * @param string $key 不传则全部清除，可选参数 select,select_adv,from,join,where,group_by,having,set,columns,values,where,index,order_by,distinct,limit,offset,table,last_join,join,on
     * @return $this
     */
    public function reset($key = null)
    {
        if ($key)
        {
            foreach ((array)$key as $item)
            {
                $key = strtolower($key);
                switch ($key)
                {
                    case 'distinct':
                        $this->_builder['distinct'] = false;
                        break;
                    case 'limit':
                    case 'offset':
                    case 'table':
                        $this->_builder[$key] = null;
                        break;
                    case 'last_join':
                    case 'join':
                    case 'on':
                        $this->_builder['last_join'] = null;
                        break;
                    default:
                        if (isset($this->_builder[$key]))
                        {
                            $this->_builder[$key] = [];
                        }
                        break;
                }
            }
        }
        else
        {
            $this->_builderBak = $this->_builder;

            $this->_builder['select']
                = $this->_builder['select_adv']
                = $this->_builder['from']
                = $this->_builder['join']
                = $this->_builder['where']
                = $this->_builder['group_by']
                = $this->_builder['having']
                = $this->_builder['set']
                = $this->_builder['columns']
                = $this->_builder['values']
                = $this->_builder['where']
                = $this->_builder['index']
                = $this->_builder['group_concat']
                = $this->_builder['statement']
                = $this->_builder['order_by'] = [];

            $this->_builder['distinct'] = false;

            $this->_builder['limit']
                = $this->_builder['offset']
                = $this->_builder['table']
                = $this->_builder['last_join'] = null;
        }

        return $this;
    }

    /**
     *
     * @param string $key
     * @param array $value
     * @return $this
     */
    public function in($column, $value, $noIn = false)
    {
        return $this->andWhere($column, $value, $noIn ? 'not in' : 'in');
    }

    public function notIn($column, $value)
    {
        return $this->andWhere($column, $value, 'not in');
    }

    /**
     * Alias of and_where()
     *
     * @param  mixed $column column name or array($column, $alias) or object
     * @param  string $value logic operator
     * @param  mixed $op column value
     * @return $this
     */
    public function where($column, $value = null, $op = '=')
    {
        if (is_array($column))
        {
            foreach ($column as $c => $value)
            {
                $this->andWhere($c, $value, $op);
            }
            return $this;
        }
        return $this->andWhere($column, $value, $op);
    }

    /**
     * Creates a new "AND WHERE" condition for the query.
     *
     * @param  mixed $column column name or array($column, $alias) or object
     * @param  string $value logic operator
     * @param  mixed $op column value
     * @return $this
     */
    public function andWhere($column, $value, $op = '=')
    {
        if (!is_object($column))
        {
            $column = trim($column);
            if (preg_match('#^(.*)(>|<|>=|<=|\!=|<>)$#', $column, $m))
            {
                $column = $m[1];
                $op     = $m[2];
            }
        }
        $this->_builder['where'][] = ['AND' => [$column, $op, $value]];

        return $this;
    }

    /**
     * Creates a new "OR WHERE" condition for the query.
     *
     * @param  mixed $column column name or array($column, $alias) or object
     * @param  string $value logic operator
     * @param  mixed $op column value
     * @return $this
     */
    public function orWhere($column, $value, $op = '=')
    {
        $this->_builder['where'][] = ['OR' => [$column, $op, $value]];

        return $this;
    }

    /**
     * Alias of and_where_open()
     *
     * @return $this
     */
    public function whereOpen()
    {
        return $this->andWhereOpen();
    }

    /**
     * Opens a new "AND WHERE (...)" grouping.
     *
     * @return $this
     */
    public function andWhereOpen()
    {
        $this->_builder['where'][] = ['AND' => '('];

        return $this;
    }

    /**
     * Opens a new "OR WHERE (...)" grouping.
     *
     * @return $this
     */
    public function orWhereOpen()
    {
        $this->_builder['where'][] = ['OR' => '('];

        return $this;
    }

    /**
     * Closes an open "AND WHERE (...)" grouping.
     *
     * @return $this
     */
    public function whereClose()
    {
        return $this->andWhereClose();
    }

    /**
     * Closes an open "AND WHERE (...)" grouping.
     *
     * @return $this
     */
    public function andWhereClose()
    {
        $this->_builder['where'][] = ['AND' => ')'];

        return $this;
    }

    /**
     * Closes an open "OR WHERE (...)" grouping.
     *
     * @return $this
     */
    public function orWhereClose()
    {
        $this->_builder['where'][] = ['OR' => ')'];

        return $this;
    }

    /**
     * Applies sorting with "ORDER BY ..."
     *
     * @param  mixed $column column name or array($column, $alias) or object
     * @param  string $direction direction of sorting
     * @return $this
     */
    public function orderBy($column, $direction = 'ASC')
    {
        $this->_builder['order_by'][] = array($column, strtoupper($direction));

        return $this;
    }

    /**
     * Return up to "LIMIT ..." results
     *
     * @param  integer $number maximum results to return
     * @param  integer $offset maximum results from offset
     * @return $this
     */
    public function limit($number, $offset = null)
    {
        $this->_builder['limit'] = (int)$number;

        if (null !== $offset)
        {
            $this->offset($offset);
        }

        return $this;
    }

    /**
     * 返回 "LIKE ..."
     *
     * @param string $column
     * @param string $value
     * @return $this
     */
    public function like($column, $value = null)
    {
        return $this->where($column, $value, 'like');
    }

    /**
     * 返回 "OR LIKE ..."
     *
     * @param string $column
     * @param string $value
     * @return $this
     */
    public function orLike($column, $value = null)
    {
        return $this->orWhere($column, $value, 'like');
    }

    /**
     * 返回 "$column MOD $mod = $value"
     *
     * @param string $column
     * @param int $mod_dig
     * @param int $value
     * @return $this
     */
    public function mod($column, $mod_dig, $value)
    {
        return $this->andWhere($column, array($mod_dig, $value), 'mod');
    }

    /**
     * 返回 "OR $column MOD $mod = $value"
     *
     * @param string $column
     * @param int $mod_dig
     * @param int $value
     * @return $this
     */
    public function orMod($column, $mod_dig, $value, $op = '=')
    {
        return $this->orWhere($column, array($mod_dig, $value, $op), 'mod');
    }

    /**
     * 使用指定索引
     *
     * @param string
     * @return $this
     */
    public function useIndex($index)
    {
        $this->_builder['index'][] = array($index, 'use');

        return $this;
    }

    /**
     * 强制使用指定索引
     *
     * @param string
     * @return $this
     */
    public function forceIndex($index)
    {
        $this->_builder['index'][] = array($index, 'force');

        return $this;
    }

    /**
     * 或略指定索引
     *
     * @param string
     * @return $this
     */
    public function ignoreIndex($index)
    {
        $this->_builder['index'][] = array($index, 'ignore');

        return $this;
    }

    /**
     * 恢复最后查询或reset时的Builder数据
     *
     * 此方法等同于在执行查询前先获取 `$builder = $db->getBuilder();` 然后执行SQL完毕后把原先的builder重新设置 `$db->setBuilder($builder);`
     *
     *      $db->from('my_db')->where('id', 1)->get()->asArray();    // 执行查询
     *      $db->recoveryLastBuilder();                              // 恢复
     *
     * 等同于下面代码，但明显上面代码更优雅
     *
     *      $db->from('my_db')->where('id', 1);
     *
     *      $builder = $db->getBuilder();      // 在执行前获取builder设置
     *      $db->get()->asArray();             // 执行查询
     *      $db->setBuilder($builder);         // 将前面获取的builder重新复原
     *
     * 例子一
     *
     *      $count = $db->from('my_db')->where('id', 10, '>')->count_records();
     *      // 在执行count_records()时，所有的builder数据将会被清空
     *      echo $db->lastQuery();   // SELECT COUNT(1) AS `total_row_count` FROM `mydb` WHERE `id` > '10'
     *
     *      // 恢复builder
     *      $db->recoveryLastBuilder();
     *      $db->limit(20)->orderBy('id', 'DESC')->get()->asArray();
     *
     *      echo $db->last_query();   // SELECT * FROM `my_db` WHERE `id` > '10' ORDER BY `id` DESC LIMIT 10
     *
     *
     * @return $this
     */
    public function recoveryLastBuilder()
    {
        if ($this->_builderBak)
        {
            $this->_builder = $this->_builderBak;
        }

        return $this;
    }

    /**
     * 创建一个不会被过滤处理的字符串
     *
     * @param string|array expression
     * @return $this_Expression
     */
    public static function exprValue($string)
    {
        return new Expression($string);
    }
}