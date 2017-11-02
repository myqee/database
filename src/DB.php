<?php
namespace MyQEE\Database;

# 是否有 MyQEE 基础类库
define('INCLUDE_MYQEE_CORE', class_exists('\\MyQEE', false));


/**
 * 数据库核心类
 *
 * @author     呼吸二氧化碳 <jonwang@myqee.com>
 * @category   Database
 * @copyright  Copyright (c) 2008-2018 myqee.com
 * @license    http://www.myqee.com/license.html
 */
class DB extends QueryBuilder
{
    /**
     * 驱动对象
     *
     * @var Driver
     */
    protected $driver;

    /**
     * @var array Database instances
     */
    protected static $instances = [];

    /**
     * 当前配置
     *
     * @var array
     */
    protected $config;

    /**
     * 是否自动使用主数据库
     *
     * @var boolean
     */
    protected $isAutoUseMaster = false;

    /**
     * 记录慢查询
     *
     *     [
     *         # 执行时的时间   耗时(单位毫秒)   查询语句
     *         [1351691389,   1200          ,'select * from test;']
     *     ]
     *
     * @var array
     */
    protected static $slowQueries = [];

    /**
     * 所有的配置
     *
     * @var array
     */
    protected static $allConfig = [];

    /**
     * 默认配置名
     *
     * @var string
     */
    const DEFAULT_CONFIG_NAME = 'default';


    /**
     * 返回数据库实例化对象
     *
     * 支持 `$db = DB::instance('mysql://root:123456@127.0.0.1/myqee/');` 的方式
     *
     * @param string $configName 默认值为 Database::DEFAULT_CONFIG_NAME
     * @return static
     */
    public static function instance($configName = null)
    {
        if (null === $configName)
        {
            $configName = static::DEFAULT_CONFIG_NAME;
        }

        if (is_string($configName))
        {
            $name = $configName;
        }
        else
        {
            $name = '.config.' . md5(serialize($configName));
        }

        if (!isset(static::$instances[$name]))
        {
            static::$instances[$name] = new DB($configName);

            if (INCLUDE_MYQEE_CORE)
            {
                # 注册服务
                if ($configName === static::DEFAULT_CONFIG_NAME)
                {
                    \MyQEE\Service::register('$db', static::$instances[$name], false);
                }
                elseif (is_string($configName))
                {
                    \MyQEE\Service::register('$db.' . $configName, static::$instances[$name], false);
                }
            }
        }

        return static::$instances[$name];
    }

    /**
     * new DB('default');
     *
     * 支持 `new DB('mysql://root:123456@127.0.0.1/myqee/');` 的方式
     *
     * @param string $configName 默认值为 `Database::DEFAULT_CONFIG_NAME`
     * @return  void
     */
    public function __construct($configName = null)
    {
        parent::__construct();

        if (null === $configName)
        {
            $configName = static::DEFAULT_CONFIG_NAME;
        }

        if (is_array($configName))
        {
            $this->config = $configName;
        }
        elseif (false !== strpos($configName, '://'))
        {
            list($type) = explode('://', $configName);

            $this->config = [
                'type'       => $type,
                'connection' => $configName,
                'prefix'     => '',
                'charset'    => 'UTF8',
                'caching'    => false,
                'profiling'  => false,
            ];
        }
        elseif (static::$allConfig && isset(static::$allConfig[$configName]))
        {
            $this->config = static::$allConfig[$configName];
        }
        elseif (INCLUDE_MYQEE_CORE)
        {
            $this->config = \MyQEE\config('database.' . $configName);
        }
        else
        {
            throw new Exception('Can not found database config');
        }

        $this->config['charset'] = strtoupper($this->config['charset']);

        # 自动转换数据库编码到UTF8
        if (!isset($this->config['autoConvertToUtf8']))
        {
            $this->config['autoConvertToUtf8'] = false;
        }

        if ($this->config['autoConvertToUtf8'])
        {
            # 数据库原始数据
            if (isset($this->config['dataCharset']))
            {
                $this->config['dataCharset'] = strtoupper($this->config['dataCharset']);
            }
            else
            {
                $this->config['dataCharset'] = $this->config['charset'];
            }
        }

        if (isset($this->config['driverClass']) && $this->config['driverClass'])
        {
            # 自定义对象名
            $driver = $this->config['driverClass'];
        }
        else
        {
            switch (strtolower($this->config['type']))
            {
                case 'mysql':
                case 'mysqli':
                case 'maria':
                case 'mariadb':
                    $driver = 'MariaDB';
                    break;

                case 'mongo':
                case 'mongodb':
                    $driver = 'MongoDB';
                    break;

                case 'pdo':
                    $driver = 'PDO';
                    break;

                case 'postgre':
                case 'postgresql':
                    $driver = 'PostgreSQL';
                    break;

                case 'sqlite':
                    $driver = 'SQLite';
                    break;

                default:
                    $driver = ucfirst($this->config['type']);
            }

            if (!$driver)
            {
                $driver = 'MariaDB';
            }

            $this->config['type'] = $driver;
            $driver = "\\MyQEE\\Database\\{$driver}\\Driver";
        }

        if (!class_exists($driver, true))
        {
            throw new Exception("Database Driver: $driver not found.");
        }

        if (!isset($this->config['connection']))
        {
            throw new Exception('Database connection not set.');
        }

        if (is_string($this->config['connection']))
        {
            $this->config['connection'] = static::parseDsn($this->config['connection']);

            unset($this->config['connection']['type']);
            if (isset($this->config['connection']['extra']))
            {
                $extra = $this->config['connection']['extra'];
                unset($this->config['connection']['extra']);
                unset($extra['connection']);
                $this->config = array_merge($this->config, $extra);
            }
        }

        # 当前驱动
        $this->driver = new $driver($this->config);

        if (INCLUDE_MYQEE_CORE)
        {
            # 增加自动关闭连接列队
        }
    }

    public function __destruct()
    {
        $this->closeConnect();
    }

    /**
     * 获取驱动引擎对象
     *
     * @return Driver
     */
    public function driver()
    {
        return $this->driver;
    }

    /**
     * 获取当前配置数组
     *
     * @return array
     */
    public function config()
    {
        return $this->config;
    }

    /**
     * 关闭连接
     *
     * @return $this
     */
    public function closeConnect()
    {
        if ($this->driver)
        {
            $this->driver->closeConnect();
        }

        return $this;
    }

    /**
     * 安全的执行SQL模板查询
     *
     * 需要先通过 `$this->prepare($statement)` 设置SQL后执行
     * 如果当前驱动是PDO，则使用PDO相同的方式处理，
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
     * @param array $inputParameters
     * @param bool $asObject
     * @param null $connectionType
     * @return Result
     */
    public function execute(array $inputParameters = [], $asObject = false, $connectionType = null)
    {
        if (!$this->builder['statement'])
        {
            throw new Exception('You need run `$db->prepare($statement)` before execute.');
        }

        if (null === $connectionType && true === $this->isAutoUseMaster)
        {
            $connectionType = true;
        }

        $time = $this->startSlowQuery();

        $rs = $this->driver->execute($this->builder['statement'], $inputParameters, $asObject, $connectionType);

        if (false !== $time)
        {
            $this->recordSlowQuery($time);
        }

        return $rs;
    }

    /**
     * 执行SQL查询
     *
     * [!!] 此方法的SQL语句将不进行SQL注入过滤，执行语句请慎重，建议通过数据库的QueryBuilder来构造SQL语句，如果此方法无法满足要求也只用 `$db->execute()` 方法
     *
     *      $db->query('select * from my_table where id = 10');
     *
     * @param string|DB|QueryBuilder $sql
     * @param boolean $asObject 返回对象名称 默认false，即返回数组
     * @param string $clusterName 使用集群, true 则使用主数据库, 例如 slave, master 等, 确保数据库相关配置中包括对应集群
     * @return Result|int|array
     */
    public function query($sql, $asObject = false, $clusterName = null)
    {
        if (null === $clusterName && true === $this->isAutoUseMaster)
        {
            $clusterName = true;
        }

        $time = $this->startSlowQuery();

        $rs = $this->driver->query($sql, $asObject, $clusterName);

        if (false !== $time)
        {
            $this->recordSlowQuery($time);
        }

        return $rs;
    }

    /**
     * 返回当前表前缀
     *
     * @return  string
     */
    public function prefix()
    {
        return $this->config['prefix'];
    }

    /**
     * 解析为SQL语句
     *
     * @see QueryBuilder::compile()
     * @param string $type select, insert, update, delete, replace
     * @return  string
     */
    public function compile($type = 'select')
    {
        # 获取查询SQL
        $sql = $this->driver->compile($this->getAndResetBuilder(), $type);

        return $sql;
    }

    /**
     * 获取数据
     *
     * @param boolean $asObject 返回对象名称 默认false，即返回数组
     * @param string $clusterName 使用集群, true 则使用主数据库, 例如 slave, master 等, 确保数据库相关配置中包括对应集群
     * @return Result
     */
    public function get($asObject = false, $clusterName = null)
    {
        return $this->query($this->compile(), $asObject, $clusterName);
    }

    /**
     * 获取一条数据
     *
     * @param boolean $asObject 返回对象名称 默认false，即返回数组
     * @param string $clusterName 使用集群, true 则使用主数据库, 例如 slave, master 等, 确保数据库相关配置中包括对应集群
     * @return array|object|\stdClass
     */
    public function getSingle($asObject = false, $clusterName = null)
    {
        return $this->limit(1)->get($asObject, $clusterName)->current();
    }

    /**
     * 最后查询的SQL语句
     *
     * @return string
     */
    public function lastQuery()
    {
        return $this->driver->lastQuery();
    }

    /**
     * 更新数据
     *
     * @param string $table
     * @param array $value
     * @param array $where
     * @return int 作用的行数
     */
    public function update($table = null, $value = null, $where = null)
    {
        if ($table)
        {
            $this->table($table);
        }

        if ($value)
        {
            $this->set($value);
        }

        if ($where)
        {
            $this->where($where);
        }

        $sql = $this->compile('update');

        return $this->query($sql, false, true);
    }

    /**
     * 插入数据
     *
     * @param string $table
     * @param array $value
     * @param Result
     * @return array  array(插入ID, 作用行数)
     */
    public function insert($table = null, $value = null)
    {
        if ($table)
        {
            $this->table($table);
        }

        if ($value)
        {
            $this->columns(array_keys($value));
            $this->values($value);
        }

        $sql = $this->compile('insert');

        return $this->query($sql, false, true);
    }

    /**
     * 删除数据
     *
     * @param string $table 表名称
     * @param array $where 条件
     * @return integer 操作行数
     */
    public function delete($table = null, $where = null)
    {
        if ($table)
        {
            $this->table($table);
        }

        if ($where)
        {
            $this->where($where);
        }

        $sql = $this->compile('delete');

        return $this->query($sql, false, true);
    }

    /**
     * 统计指定条件的数量
     *
     * @param mixed $table table name string or array(query, alias)
     * @param array $where Where条件
     * @return  integer
     */
    public function total($table = null, $where = null)
    {
        return $this->countRecords($table, $where);
    }

    /**
     * 统计指定条件的数量
     *
     * @param mixed $table table name string or array(query, alias)
     * @param array $where Where条件
     * @return  integer
     */
    public function countRecords($table = null, $where = null)
    {
        if ($table)
        {
            $this->from($table);
        }
        if ($where)
        {
            $this->where($where);
        }

        $this->select($this->exprValue('COUNT(1) AS `totalRowCount`'));

        $count = (int)$this->query($this->compile('select'), false)->get('totalRowCount');

        return $count;
    }

    /**
     * 替换数据 REPLACE INTO
     *
     * `$update_on_duplicate_mode` 参数只对MySQL有效
     *  默认 `false` 使用传统的 REPLACE INTO 语句执行
     *  当设置成 `true` 后，系统将用 `INSERT INTO .... ON DUPLICATE KEY UPDATE ... ` 的语句方式执行，而不是直接REPLACE INTO语句
     *  详情见 [https://dev.mysql.com/doc/refman/5.0/en/insert-on-duplicate.html] 页面
     *
     *
     * @param string $table
     * @param array $value
     * @param array $where
     * @param array $insertOnDuplicateKeyUpdateMode 只有MySQL支持
     * @param Result
     */
    public function merge($table = null, $value = null, $where = null, $insertOnDuplicateKeyUpdateMode = false)
    {
        return $this->replace($table, $value, $where, $insertOnDuplicateKeyUpdateMode);
    }

    /**
     * 替换数据 REPLACE INTO
     *
     * `$update_on_duplicate_mode` 参数只对MySQL有效
     *  默认 `false` 使用传统的 REPLACE INTO 语句执行
     *  当设置成 `true` 后，系统将用 `INSERT INTO .... ON DUPLICATE KEY UPDATE ... ` 的语句方式执行，而不是直接REPLACE INTO语句
     *  详情见 [https://dev.mysql.com/doc/refman/5.0/en/insert-on-duplicate.html] 页面
     *
     * @param string $table
     * @param array $value
     * @param array $where
     * @param array $insertOnDuplicateKeyUpdateMode 只有MySQL支持
     * @param Result
     */
    public function replace($table = null, $value = null, $where = null, $insertOnDuplicateKeyUpdateMode = false)
    {
        if ($table)
        {
            $this->table($table);
        }

        if ($value)
        {
            $this->columns(array_keys($value));
            $this->values($value);
        }

        if ($where)
        {
            $this->where($where);
        }

        $sql = $this->compile($insertOnDuplicateKeyUpdateMode ? 'insert_update' : 'replace');

        return $this->query($sql, false, true);
    }

    /**
     * 获取事务对象
     *
     * @return Transaction 事务对象
     */
    public function transaction()
    {
        return $this->driver->transaction();
    }

    /**
     * 返回当前事务处理的类名称
     *
     * 不支持则返回 false
     *
     * @return string|false
     */
    public function transactionClassName()
    {
        return $this->driver->transactionClassName();
    }

    /**
     * 设置是否一直在主数据库上查询
     *
     * 这样设置后，select会一直停留在主数据库上，直到$this->auto_use_master(false)后才会自动判断
     * @param boolean $autoMaster
     * @return $this
     */
    public function autoUseMaster($autoMaster = true)
    {
        $this->isAutoUseMaster = (bool)$autoMaster;

        return $this;
    }

    /**
     * 是否一直用主数据库查询
     *
     * @return boolean
     */
    public function isAutoUseMaster()
    {
        return $this->isAutoUseMaster;
    }

    /**
     * 创建一个数据库
     *
     * @param string $database
     * @param string $charset 编码，不传则使用数据库连接配置相同到编码
     * @param string $collate 整理格式
     * @return boolean
     * @throws Exception
     */
    public function createDatabase($database, $charset = null, $collate = null)
    {
        if (method_exists($this->driver, 'createDatabase'))
        {
            return $this->driver->createDatabase($database, $charset, $collate);
        }
        else
        {
            return false;
        }
    }

    /**
     * 返回是否支持对象数据
     *
     * 通常传统的数据库是不支持直接存储对象数据的，而MongoDB是支持的
     *
     * @return bool
     */
    public function isSupportObjectValue()
    {
        return $this->driver->isSupportObjectValue();
    }

    /**
     * 开启记录慢查询
     *
     * 返回当前时间，如果系统设置关闭记录慢查询，则返回 false
     *
     * @return bool|mixed
     */
    protected function startSlowQuery()
    {
        if (static::getSlowQuerySettingTime() > 0)
        {
            return microtime(1);
        }
        else
        {
            return false;
        }
    }

    /**
     * 保存慢查询日志
     *
     * @param float $startTime 由 `$this->_start_slow_query()` 返回的值
     */
    protected function recordSlowQuery($startTime)
    {
        if (!$startTime)
        {
            return;
        }

        $endTime = microtime(1);
        $useTime = 1000 * ($endTime - $startTime);

        if (($minTime = static::getSlowQuerySettingTime()) && $useTime > $minTime)
        {
            // 记录慢查询
            static::$slowQueries[] = [$startTime, $useTime, $this->lastQuery()];
        }
    }

    /**
     * 加载文本数据库配置
     *
     * 文本配置内容范例
     *
     *      [default]
     *      ;连接1
     *      mysql://root:123456@127.0.0.1:3306/test/?charset=utf8
     *
     *      [mongo]
     *      ;连接2
     *      mongo://127.0.0.1/test/?charset=utf8
     *
     *      ; 连接1的从库
     *      [default.slave]
     *      mysql://192.168.1.3/?weight=30
     *      mysql://user1:pass1@192.168.1.5:3307/
     *      mysql://user:pass@192.168.1.4:3333/
     *
     *      ; 连接1的搜索库
     *      [default.search]
     *      mysql://192.168.1.5/?weight=30
     *      mysql://192.168.1.6/?weight=40
     *      mysql://192.168.1.7/?weight=40
     *      mysql://192.168.1.7/?weight=50
     *
     *      ; 连接2的从库
     *      [mongo.slave]
     *      mongo://192.168.1.2/test
     *      mongo://192.168.1.3/test
     *      mongo://192.168.1.4/test
     *
     * @param $file
     * @return array|bool 返回解析后的config, 并记录在 `DB::$allConfig` 变量中
     */
    public static function loadConfig($file)
    {
        if (!is_file($file))return false;

        $contents = explode("\n", trim(file_get_contents($file)));

        $config        = [];
        $configCluster = [];
        $configName    = null;
        $clusterName   = null;
        $isCluster     = false;

        foreach ($contents as $item)
        {
            $item = trim($item);

            if ('' === $item || in_array(substr($item, 0, 1), [';', '#']))
            {
                # 注释
                continue;
            }

            if (preg_match('#^\[([a-z0-9_\.]+)\]$#i', $item, $m))
            {
                if (false !== strpos($m[1], '.'))
                {
                    $isCluster  = true;
                    list($configName, $clusterName) = explode('.', $m[1]);
                }
                else
                {
                    $isCluster  = false;
                    $configName = $m[1];
                }
            }
            elseif (null === $configName)
            {
                continue;
            }
            else
            {
                if (false !== strpos($item, '://'))
                {
                    $dbConfig = static::parseDsn($item);
                }
                elseif (isset($config[$configName]))
                {
                    # 字符串
                    $masterConfig = $config[$configName]['connection'];

                    $dbConfig = [
                        'username'   => $masterConfig['username'],
                        'password'   => $masterConfig['password'],
                        'hostname'   => $item,
                        'port'       => $masterConfig['port'],
                        'persistent' => $masterConfig['persistent'],
                        'database'   => $masterConfig['database'],
                        'weight'     => 10,
                    ];
                }
                else
                {
                    continue;
                }

                if ($dbConfig)
                {
                    if ($isCluster)
                    {
                        if (!isset($dbConfig['weight']))
                        {
                            $dbConfig['weight'] = 10;
                        }

                        unset($dbConfig['type']);
                        $configCluster[$configName][$clusterName][] = $dbConfig;
                    }
                    else
                    {
                        if (isset($dbConfig['extra']))
                        {
                            $config[$configName] = $dbConfig['extra'];
                            unset($dbConfig['extra']);
                        }

                        $config[$configName]['type'] = $dbConfig['type'];

                        unset($dbConfig['type']);

                        $config[$configName]['connection'] = $dbConfig;
                    }

                }
            }
        }

        if ($configCluster)foreach ($configCluster as $key => $item)
        {
            $config[$key]['cluster'] = $item;
        }

        static::$allConfig = $config;

        return $config;
    }

    /**
     * 解析DSN路径格式
     *
     * @param  string $dsn DSN string
     * @return array
     */
    public static function parseDsn($dsn)
    {
        $db = [
            'type'       => false,
            'username'   => false,
            'password'   => false,
            'hostname'   => false,
            'port'       => false,
            'persistent' => false,
            'database'   => false,
        ];

        // Get the protocol and arguments
        list ($db['type'], $connection) = explode('://', $dsn, 2);

        if ($connection[0] === '/')
        {
            // Strip leading slash
            $db['database'] = substr($connection, 1);
        }
        else
        {
            $connection = parse_url('http://' . $connection);

            if (isset($connection['user']))
            {
                $db['username'] = $connection['user'];
            }

            if (isset($connection['pass']))
            {
                $db['password'] = $connection['pass'];
            }

            if (isset($connection['port']))
            {
                $db['port'] = $connection['port'];
            }

            if (isset($connection['host']))
            {
                if ($connection['host'] === 'unix(')
                {
                    list ($db['persistent'], $connection['path']) = explode(')', $connection['path'], 2);
                }
                else
                {
                    $db['hostname'] = $connection['host'];
                }
            }

            if (isset($connection['path']) && $connection['path'])
            {
                // Strip leading slash
                $db['database'] = trim(trim(substr($connection['path'], 1), '/'));
            }

            if (isset($connection['query']))
            {
                parse_str($connection['query'], $extra);

                if (isset($extra['weight']))
                {
                    $db['weight'] = $extra['weight'];
                    unset($extra['weight']);
                }

                if ($extra)
                {
                    $db['extra'] = $extra;
                }
            }
        }

        return $db;
    }

    /**
     * 关闭全部数据库链接
     */
    public static function closeAllConnect()
    {
        if (!static::$instances || !is_array(static::$instances))
        {
            return;
        }

        foreach (static::$instances as $database)
        {
            if ($database instanceof DB)
            {
                $database->closeConnect();
            }
        }

        // 执行保存慢查询方法
        static::saveSlowQuery();
    }

    /**
     * 记录慢查询
     *
     * @return boolean
     */
    protected static function saveSlowQuery()
    {
        if (!static::$slowQueries)
        {
            return true;
        }

        $queries = [];
        foreach (static::$slowQueries as $item)
        {
            $queries[] = [
                'from' => $item[0],
                'to'   => $item[1],
                'use'  => $item[1] - $item[0],
                'sql'  => $item[2],
            ];
        }

        $data = [
            'url'      => $_SERVER["SCRIPT_URI"] . ('' !== $_SERVER["QUERY_STRING"] ? '?' . $_SERVER["QUERY_STRING"] : ''),
            'method'   => isset($_SERVER['REQUEST_METHOD']) ? $_SERVER['REQUEST_METHOD'] : 'null',
            'time'     => $_SERVER['REQUEST_TIME'],
            'ip'       => self::getIp(),
            'pageTime' => microtime(1) - $_SERVER['REQUEST_TIME_FLOAT'],
            'post'     => $_POST,
            'get'      => $_GET,
            'cookie'   => $_COOKIE,
            'queries'  => $queries,
        ];

        // 写入LOG
        return self::log('database.slowQuery', $data, LOG_INFO);
    }

    /**
     * 写入Log
     *
     * @param       $tag
     * @param array $data
     * @param int   $type
     * @return bool
     */
    protected static function log($tag, array $data, $type = LOG_INFO)
    {
        if (true === INCLUDE_MYQEE_CORE)
        {
            return \MyQEE\log('database.slowQuery', $data, LOG_INFO);
        }
        else
        {
            return syslog($type, "[$tag] ". json_encode($data, JSON_UNESCAPED_UNICODE));
        }
    }

    protected static function getSlowQuerySettingTime()
    {
        static $slowQueryMTime = null;

        if (null === $slowQueryMTime)
        {
            if (!INCLUDE_MYQEE_CORE)
            {
                if (PHP_SAPI === 'cli')
                {
                    $slowQueryMTime = false;
                }
                else
                {
                    $slowQueryMTime = 3000;
                }
            }
            elseif (IS_CLI)
            {
                $slowQueryMTime = false;
            }
            else
            {
                $slowQueryMTime = (int)\MyQEE\config('core.slowQueryMtime');
            }
        }

        return $slowQueryMTime;
    }

    /**
     * 获取IP
     *
     * @return string
     */
    protected static function getIp()
    {
        $ip = [];

        if (isset($_SERVER['HTTP_X_FORWARDED_FOR']) && $_SERVER['HTTP_X_FORWARDED_FOR'])
        {
            $ip = explode(',', str_replace(' ', '', $_SERVER['HTTP_X_FORWARDED_FOR']));
        }

        if (isset($_SERVER['HTTP_CLIENT_IP']) && $_SERVER['HTTP_CLIENT_IP'])
        {
            $ip = array_merge($ip, explode(',', str_replace(' ', '', $_SERVER['HTTP_CLIENT_IP'])));
        }

        if (isset($_SERVER['REMOTE_ADDR']) && $_SERVER['REMOTE_ADDR'])
        {
            $ip = array_merge($ip, explode(',', str_replace(' ', '', $_SERVER['REMOTE_ADDR'])));
        }

        if (!$ip)return 'unknown';
        return current($ip);
    }
}
