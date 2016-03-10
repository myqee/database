<?php
namespace MyQEE\Database;

/**
 * 数据库驱动核心类
 *
 * @author     呼吸二氧化碳 <jonwang@myqee.com>
 * @category   Database
 * @copyright  Copyright (c) 2008-2016 myqee.com
 * @license    http://www.myqee.com/license.html
 */
abstract class Driver
{
    /**
     * 当前连接类型 master|slave
     *
     * @var string
     */
    protected $connectionType = 'slave';

    /**
     * 当前连接的所有的ID
     *
     *     [
     *        'master' => 'abcdef...',
     *        'slave'  => 'defdef...',
     *     ]
     *
     * @var array
     */
    protected $connectionIds = [
        'master' => null,
        'slave'  => null,
    ];

    /**
     * 最后查询SQL语句
     *
     * @var string
     */
    protected $lastQuery = '';

    /**
     * 当前配置
     * @var array
     */
    protected $config;

    /**
     * 字符串引用符号
     *
     * @var string
     */
    protected $identifier = '"';

    /**
     * 默认端口
     *
     * @var int
     */
    protected $defaultPort = null;

    protected $_asTable = [];

    /**
     * 引擎是MySQL
     *
     * @var bool
     */
    protected $mysql = false;

    /**
     * 注册器对象列表
     *
     * @var array
     */
    protected $injectors = [];

    /**
     * 事件列表
     *
     * @var array
     */
    protected $events = [];

    /**
     * Before事件列表
     *
     * @var array
     */
    protected $eventsBefore = [];

    /**
     * After事件列表
     *
     * @var array
     */
    protected $eventsAfter = [];

    /**
     * 记录事务
     * array(
     * '连接ID'=>'父事务ID',
     * '连接ID'=>'父事务ID',
     * ...
     * )
     * @var array
     */
    protected static $transactions = [];

    /**
     * 记录hash对应的host数据
     * @var array
     */
    protected static $hashToHostName = [];

    public function __construct(array $config)
    {
        $this->config = $config;

        if (!is_array($this->config['connection']['hostname']))
        {
            # 主从链接采用同一个内存地址
            $this->connectionIds['master'] =& $this->connectionIds['slave'];
        }

        if ($this->defaultPort && (!isset($this->config['connection']['port']) || !$this->config['connection']['port'] > 0))
        {
            $this->config['connection']['port'] = $this->defaultPort;
        }
    }

    public function __destruct()
    {
        $this->closeConnect();
    }

    /**
     * 执行构造语法执行
     *
     * @param string $statement
     * @param array $inputParameters
     * @param null|bool|string $asObject
     * @param null|bool|string $connectionType
     * @return Result
     */
    public function execute($statement, array $inputParameters, $asObject = null, $connectionType = null)
    {
        $num_parameters = array();
        foreach($inputParameters as $key => $value)
        {
            if (is_int($key))
            {
                $num_parameters[$key] = $value;
            }
            else
            {
                $statement = str_replace($key, $this->quote($value), $statement);
            }
        }

        if ($num_parameters)
        {
            # 用 ? 分割开
            $statementArray = explode('?', $statement);

            # 填补缺失的key，例如 $num_parameters = array(0=>'a', 2=>'b'); 缺失了 1
            foreach($statementArray as $key => $value)
            {
                if (!isset($statementArray[$key]))$statementArray[$key] = '?';
            }

            foreach($num_parameters as $key => $value)
            {
                $statementArray[$key] = $this->quote($value) . $statementArray[$key];
            }

            # 拼接
            $statement = implode('', $statementArray);
        }

        return $this->query($statement, $asObject, $connectionType);
    }

    /**
     * 构建SQL语句
     */
    public function compile($builder, $type = 'select')
    {
        switch ($type)
        {
            case 'insert':
                return $this->compileInsert($builder);

            case'replace':
                return $this->compileInsert($builder, 'REPLACE');

            case'insert_update':
                return $this->compileInsert($builder, 'REPLACE', true);

            case 'update':
                return $this->compileDpdate($builder);

            case 'delete':
                return $this->compileDelete($builder);

            default:
                return $this->compileSelect($builder);
        }
    }

    /**
     * 查询
     * @param string $sql 查询语句
     * @param bool|string $asObject 是否返回对象
     * @param bool|string $useMaster 是否使用主数据库，不设置则自动判断
     * @return Result
     */
    abstract public function query($sql, $asObject = null, $useMaster = null);

    /**
     * 连接数据库
     *
     * $useConnectionType 默认不传为自动判断，可传true/false,若传字符串(只支持a-z0-9的字符串)，则可以切换到另外一个连接，比如传other,则可以连接到 `$this->connectionOtherId` 所对应的ID的连接
     *
     * @param boolean $useConnectionType 是否使用主数据库
     */
    abstract public function connect($useConnectionType = null);

    /**
     * 关闭链接
     */
    abstract public function closeConnect();

    /**
     * Sanitize a string by escaping characters that could cause an SQL
     * injection attack.
     *
     * $value = $db->escape('any string');
     *
     * @param  string $value  value to quote
     * @return string
     */
    abstract public function escape($value);

    /**
     * Quote a database table name and adds the table prefix if needed.
     *
     * $table = $db->quote_table($table);
     *
     * @param   mixed  $value table name or array(table, alias)
     * @param   bool  $autoAsTable
     * @return  string
     * @uses    DB::quoteIdentifier
     * @uses    DB::tablePrefix
     */
    public function quoteTable($value, $autoAsTable = false)
    {
        // Assign the table by reference from the value
        if (is_array($value))
        {
            $table = & $value[0];
        }
        else
        {
            $table = & $value;
        }

        if ($this->config['table_prefix'] && is_string($table) && strpos($table, '.') === false)
        {
            if (stripos($table, ' AS ') !== false)
            {
                $table = $this->config['table_prefix'] . $table;
            }
            else
            {
                $table = $this->config['table_prefix'] . $table . ($autoAsTable ? ' AS ' . $table : '');
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
        if ($value === null)
        {
            return 'NULL';
        }
        elseif ($value === true)
        {
            return "'1'";
        }
        elseif ($value === false)
        {
            return "'0'";
        }
        elseif (is_object($value))
        {
            if ($value instanceof DB)
            {
                // Create a sub-query
                return '('. $value->compile() .')';
            }
            elseif ($value instanceof Expression)
            {
                // Use a raw expression
                return $value->value();
            }
            elseif ($value instanceof \ArrayObject || $value instanceof \ArrayIterator || $value instanceof \stdClass)
            {
                return '('. implode(', ', array_map(array($this, __FUNCTION__), (array)$value)) .')';
            }
            else
            {
                // Convert the object to a string
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
     * 获取当前连接
     *
     * @return \mysqli
     */
    abstract public function connection();

    /**
     * 获取当前连接的唯一ID
     *
     * @return string
     */
    public function connectionId()
    {
        return $this->connectionIds[$this->connectionType];
    }

    /**
     * 获取事务对象
     *
     * @return Transaction
     */
    public function transaction()
    {
        $className = $this->transactionClassName();

        if (false === $className)
        {
            throw new \Exception(__('the transaction of :driver not exist.', [':driver' => $this->config['type']]));
        }

        return new $className($this);
    }

    /**
     * 返回当前事务处理的类名称
     *
     * 不支持事务则返回 false
     *
     * @return string|false
     */
    public function transactionClassName()
    {
        static $support = [];

        if (!isset($support[$this->config['type']]))
        {
            $className = '\\MyQEE\\Database\\'. $this->config['type'] .'\\Transaction';
            $support[$this->config['type']] = class_exists($className, true) ? $className : false;
        }

        return $support[$this->config['type']];
    }

    /**
     * 最后查询的SQL语句
     *
     * @return string
     */
    public function lastQuery()
    {
        return $this->lastQuery;
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
     * 返回是否支持对象数据
     *
     * @var bool
     */
    public function isSupportObjectValue()
    {
        return false;
    }

    /**
     * 获取一个随机HOST
     *
     * @param array $excludeHosts 排除的HOST
     * @param string $type 配置类型
     */
    protected function getRandHost($excludeHosts = [], $type = null)
    {
        if (!$type)$type = $this->connectionType;
        $hostname = $this->config['connection']['hostname'];

        if (!is_array($hostname))
        {
            if (in_array($hostname, $excludeHosts))
            {
                return false;
            }

            if ($excludeHosts && $type !== 'master' && in_array($hostname, $excludeHosts))
            {
                # 如果相应的slave都已不可获取，则改由获取master
                return $this->getRandHost($excludeHosts, 'master');
            }

            return $hostname;
        }

        $hostConfig = $hostname[$type];

        if (is_array($hostConfig))
        {
            if ($excludeHosts)
            {
                $hostConfig = array_diff($hostConfig, $excludeHosts);
            }

            $hostConfig = array_values($hostConfig);
            $count      = count($hostConfig);

            if ($count === 0)
            {
                if ($type !== 'master')
                {
                    return $this->getRandHost($excludeHosts, 'master');
                }
                else
                {
                    return false;
                }
            }

            # 获取一个随机链接
            $randId = mt_rand(0, $count - 1);

            return $hostConfig[$randId];
        }
        else
        {
            if (in_array($hostConfig, $excludeHosts))
            {
                return false;
            }

            return $hostConfig;
        }
    }

    /**
     * 获取链接唯一hash
     *
     * @param string $hostname
     * @param int $port
     * @param string $username
     * @return string
     */
    protected function getConnectionHash($hostname, $port, $username)
    {
        $hash = sha1(get_class($this) .'_'. $hostname .'_'. $port .'_'. $username);

        self::$hashToHostName[$hash] = [
            'host'     => $hostname,
            'port'     => $port,
            'username' => $username,
        ];

        return $hash;
    }

    /**
     * 根据数据库连接唯一hash获取数据信息
     *
     * @param string $has
     * @return array array('hostname'=>'','port'=>'','username'=>'')
     */
    protected static function getHostnameByConnectionHash($hash)
    {
        return self::$hashToHostName[$hash];
    }

    /**
     * 切换编码
     *
     * @param string $value
     */
    protected function changeCharset(& $value)
    {
        if ($this->config['auto_change_charset'] && $this->config['charset'] !== 'UTF8')
        {
            static $mb = null;

            if (null === $mb)
            {
                $mb = function_exists('\\mb_convert_encoding');
            }

            # 转换编码编码
            if ($mb)
            {
                $value = \mb_convert_encoding((string)$value, $this->config['data_charset'], 'UTF-8');
            }
            else
            {
                $value = \iconv('UTF-8', $this->config['data_charset'] . '//IGNORE', (string)$value);
            }
        }

        return $value;
    }

    /**
     * 设置连接类型
     *
     *    $use_connection_type 默认不传为自动判断，可传true/false,若传字符串(只支持a-z0-9的字符串)，则可以切换到另外一个连接，比如传other,则可以连接到$this->_connection_other_id所对应的ID的连接
     *
     * @param boolean|string $connectionType
     */
    protected function setConnectionType($connectionType)
    {
        if (true === $connectionType)
        {
            $connectionType = 'master';
        }
        elseif (false === $connectionType)
        {
            $connectionType = 'slave';
        }
        elseif (!$connectionType)
        {
            return;
        }

        $this->connectionType = $connectionType;
    }

    protected function getQueryType($sql, & $connectionType)
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

        $slave_type = ['SELECT', 'SHOW', 'EXPLAIN'];

        if ($type !== 'MASTER' && in_array($type, $slave_type))
        {
            if (true === $connectionType)
            {
                $connectionType = 'master';
            }
            else if (is_string($connectionType))
            {
                if (!preg_match('#^[a-z0-9_]+$#i', $connectionType))$connectionType = 'master';
            }
            else
            {
                $connectionType = 'slave';
            }
        }
        else
        {
            $connectionType = 'master';
        }

        return $type;
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

                $prefix = $this->config['table_prefix'];
                if ($prefix)
                {
                    // Get the offset of the table name, 2nd-to-last part
                    $offset = count($parts) - 2;

                    if (!$this->_asTable || !in_array($parts[$offset], $this->_asTable))
                    {
                        $parts[$offset] = $prefix . $parts[$offset];
                    }
                }

                foreach ($parts as & $part)
                {
                    if ($part !== '*')
                    {
                        // Quote each of the parts
                        $this->changeCharset($part);
                        $part = $this->identifier . str_replace([$this->identifier, '\\'], '', $part) . $this->identifier;
                    }
                }

                $column = implode('.', $parts);
            }
            else
            {
                $this->changeCharset($column);
                $column = $this->identifier . str_replace([$this->identifier, '\\'], '', $column) . $this->identifier;
            }
        }

        if (isset($alias))
        {
            $this->changeCharset($alias);
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
                $builder['select_adv'][] = [
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

        if (!empty($builder['group_by']))
        {
            // Add sorting
            $query .= ' GROUP BY '. implode(', ', array_map($quoteIdentifier, $builder['group_by']));
        }

        if (!empty($builder['having']))
        {
            // Add filtering conditions
            $query .= ' HAVING '. $this->compileConditions($builder['having']);
        }

        if (!empty($builder['order_by']))
        {
            // Add sorting
            $query .= ' '. $this->compileOrderBy($builder['order_by']);
        }
        elseif ($builder['where'])
        {
            # 如果查询中有in查询，采用自动排序方式
            $inQuery = null;
            foreach ($builder['where'] as $item)
            {
                if (isset($item['AND']) && $item['AND'][1] === 'in')
                {
                    if (count($item['AND'][1]) > 1)
                    {
                        # 大于1项才需要排序
                        $inQuery = $item['AND'];
                    }
                    break;
                }
            }
            if ($inQuery)
            {
                $query .= ' ORDER BY FIELD('. $this->quoteIdentifier($inQuery[0]) .', '. implode(', ', $this->quote($inQuery[2])) .')';
            }
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
        if ($this->mysql && $insertUpdate)
        {
            $type_string = 'INSERT';
        }
        else if ($type !== 'REPLACE')
        {
            $type_string = 'INSERT';
        }
        else
        {
            $type_string = $type;
        }

        $query = $type_string . ' INTO ' . $this->quoteTable($builder['table'], false);

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

            if ($this->mysql && $insertUpdate)
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

    protected function compileDpdate($builder)
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

        if (!empty($builder['order_by']))
        {
            // Add sorting
            $query .= ' '. $this->compileOrderBy($builder['order_by']);
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
        $sort = array();
        foreach ($columns as $group)
        {
            list ($column, $direction) = $group;

            if (!empty($direction))
            {
                // Make the direction uppercase
                $direction = ' '. strtoupper($direction);
            }

            $sort[] = $this->quoteIdentifier($column) . $direction;
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
        $statements = array();

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
     * 初始化所有的as_table
     */
    protected function initAsTable($builder)
    {
        $this->_asTable = array();

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
            list ($value, $alias) = $value;
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
        elseif ($this->config['table_prefix'] && strpos($value, '.') === false)
        {
            $alias = $value;
        }
        else
        {
            $alias = null;
        }

        if ($alias)
        {
            $this->_asTable[] = $alias;
        }
    }

    /**
     * 格式化高级查询参数到select里
     */
    protected function formatSelectAdv(& $builder)
    {
        if ($builder['select_adv'])foreach ($builder['select_adv'] as $item)
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
            $args_str = '';
            if (($countItem = count($item)) > 2)
            {
                for($i=2; $i < $countItem; $i++)
                {
                    $args_str .= ','. $this->quoteIdentifier($item[$i]);
                }
            }

            $builder['select'][] = [
                DB::exprValue(strtoupper($item[1]) .'('. $this->quoteIdentifier($column.$args_str) .')'),
                $alias,
            ];
        }
    }

    /**
     * 解析 GROUP_CONCAT
     *
     * @param array $arr
     * @return string
     */
    protected function formatGroupConcat(&$builder)
    {
        if ($builder['group_concat'])foreach($builder['group_concat'] as $item)
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


    /**
     * 触发一个预定义好的事件
     *
     *      $this->on('test', function(){echo "123";});
     *      $this->trigger('test');
     *      // 将会输出 123
     *
     * @param $event
     * @param array $tmpRelyObject 临时依赖对象数组
     * @return bool|mixed
     */
    public function trigger($event, array $tmpRelyObject = [])
    {
        if (isset($this->events[$event]))
        {
            # 执行前回调
            if (isset($this->eventsBefore[$event]))
            {
                foreach ($this->eventsBefore[$event] as $item)
                {
                    try
                    {
                        $this->callFromInjector($item[0], $item[1], $tmpRelyObject);
                    }
                    catch (\Exception $e)
                    {
                        trigger_error($e->getMessage(), $e->getCode());
                    }
                }
            }

            # 执行主事件
            list($injectors, $callback) = $this->events[$event];

            $rs = $this->callFromInjector($injectors, $callback, $tmpRelyObject);

            # 执行后回调
            if (isset($this->eventsAfter[$event]))
            {
                foreach ($this->eventsAfter[$event] as $item)
                {
                    try
                    {
                        $this->callFromInjector($item[0], $item[1], $tmpRelyObject);
                    }
                    catch (\Exception $e)
                    {
                        trigger_error($e->getMessage(), $e->getCode());
                    }
                }
            }

            return $rs;
        }
        else
        {
            return false;
        }
    }


    /**
     * 定义一个事件调用
     *
     * @param $event
     * @param $relyOrCallback
     * @param null $callback
     * @return $this
     */
    public function eventOn($event, $relyOrCallback, $callback = null)
    {
        if (null === $callback)
        {
            $callback  = $relyOrCallback;
            $injectors = [];
        }
        else
        {
            $injectors = (array)$relyOrCallback;
        }

        $this->events[$event] = [$injectors, $callback];

        return $this;
    }

    /**
     * 释放事件调用
     *
     * @param $event
     * @param bool|true $removeAfterAndBeforeEvent 是否同时移除after和before的事件
     * @return $this
     */
    public function eventOff($event, $removeAfterAndBeforeEvent = true)
    {
        unset($this->events[$event]);

        if ($removeAfterAndBeforeEvent)
        {
            unset($this->eventsAfter[$event]);
            unset($this->eventsBefore[$event]);
        }

        return $this;
    }

    /**
     * 立即执行一个自定义的方法
     *
     *      $this->call(['$db'], function($db){
     *          //....
     *      });
     *
     *      # 传入一个临时的依赖对象, 可以在本次临时覆盖已存在的依赖对象
     *      $this->call(['$db', '$test'], function($db, $test) {
     *          //....
     *      }, ['$test' => 123]);
     *
     *      # 如果不是要临时覆盖已存在依赖对象, 推荐使用 use 方式, 例如:
     *      $test = 123;
     *      $this->call(['$db'], function($db) use ($test) {
     *          //....
     *      });
     *
     * @param array $rely 依赖的注入对象名称
     * @param \Closure $callback 回调方法
     * @param array $tmpRelyObject 临时依赖对象数组
     * @return mixed
     */
    public function callFromInjector(array $rely, $callback, array $tmpRelyObject = [])
    {
        if ($tmpRelyObject)
        {
            # 依赖数据
            $relyObj = $this->getInjector($rely, 1);
            $obj     = [];
            foreach ($rely as $key)
            {
                $obj[] = array_key_exists($key, $tmpRelyObject) ? $tmpRelyObject[$key] : $relyObj[$key];
            }

            return call_user_func_array($callback, $obj);
        }
        else
        {
            return call_user_func_array($callback, $this->getInjector($rely));
        }
    }

    /**
     * 设置一个注入器对象
     *
     * @param string|array $injector
     * @param mixed $object
     * @return $this
     */
    public function setInjector($injector, $object = null)
    {
        # 支持数组方式
        if (is_array($injector))
        {
            foreach ($injector as $key => $value)
            {
                $this->setInjector($key, $value);
            }

            return $this;
        }

        if (is_object($object) && $object instanceof \Closure)
        {
            # 回调函数
            $run = false;
        }
        else
        {
            $run = true;
        }

        $this->injectors[$injector] = [$run, $object];

        return $this;
    }

    /**
     * 移除一个注入器
     *
     * @param $injector
     * @return $this
     */
    public function removeInjector($injector)
    {
        unset($this->injectors[$injector]);

        return $this;
    }

    /**
     * 获取一个注入器对象
     *
     * @param string|array $injector
     * @param int $flag 当 `$inject` 参数是数组时有用, 0: 仅仅list模式, 1:仅map模式, 2:包括map方式也包括list方式(list序列在前map在后)
     * @return array|null|mixed
     */
    public function getInjector($injector, $flag = 0)
    {
        if (is_array($injector))
        {
            $list    = [];
            $listMap = [];
            foreach($injector as $item)
            {
                if (isset($this->injectors[$item]))
                {
                    # 当前对象的依赖注入器
                    unset($rs);
                    $rs =& $this->injectors[$item];

                    if (!$rs[0])
                    {
                        # 调用函数方法
                        $fun   = $rs[1];
                        $rs[0] = true;
                        $rs[1] = $fun();
                    }
                    $di = $rs[1];
                }
                elseif (HAVE_MYQEE_CORE && \MyQEE\Service::exists($item))
                {
                    $di = \MyQEE\Site::instance()->injector->get($item, $flag);
                }
                else
                {
                    $di = null;
                }

                switch ($flag)
                {
                    case 1:
                    case true:
                        $list[$item] = $di;
                        break;
                    case 2:
                        $list[] = $di;
                        $listMap[$item] = $di;
                        break;
                    case 0:
                    default:
                        $list[] = $di;
                        break;
                }
            }

            if ($flag == 2)
            {
                $list += $listMap;
            }

            return $list;
        }
        else
        {
            if (isset($this->injectors[$injector]))
            {
                # 当前对象的依赖注入器
                $rs =& $this->injectors[$injector];

                if (!$rs[0])
                {
                    # 调用函数方法
                    $fun   = $rs[1];
                    $rs[0] = true;
                    $rs[1] = $fun();
                }

                return $rs[1];
            }
            elseif (HAVE_MYQEE_CORE)
            {
                return \MyQEE\Site::instance()->injector->get($injector);
            }
            else
            {
                return null;
            }
        }
    }
}