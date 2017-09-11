<?php
namespace MyQEE\Database;

/**
 * 数据库驱动核心类
 *
 * @author     呼吸二氧化碳 <jonwang@myqee.com>
 * @category   Database
 * @copyright  Copyright (c) 2008-2018 myqee.com
 * @license    http://www.myqee.com/license.html
 */
abstract class Driver
{
    /**
     * 最后查询SQL语句
     *
     * @var string
     */
    protected $lastQuery = '';

    /**
     * 数据库配置
     *
     * 单个示例:
     *
     *      $this->config = [
     *          'type'     => 'mysql',
     *          'charset'  => 'utf8',
     *          'connection' => [
     *              'hostname' => '127.0.0.1',              // 主数据库
     *              'port'     => 3306,
     *              'username' => 'root',
     *              'password' => '123456',
     *              'database' => 'test',
     *          ],
     *          'cluster'  => [
     *              'slave' => [                        // 从数据库, weight 默认值为10
     *                  '192.168.1.3',
     *                  'mysql://user:pass@192.168.1.4:3333/',
     *                  [
     *                      'username' => 'user1',
     *                      'password' => 'pass1',
     *                      'hostname' => '192.168.1.5',
     *                      'port'     => 3307,
     *                      'weight'   => 20,
     *                  ],
     *              ],
     *              'search' => [
     *                  '192.168.1.5',
     *                  '192.168.1.6',
     *                  '192.168.1.7',
     *              ],
     *          ],
     *      ]
     *
     * 使用:
     *
     *      use \MyQEE\Database\DB;
     *      # 加载数据库配置
     *      DB::loadConfig('/path/of/config.conf');
     *      # 使用
     *      $db1 = DB();
     *      $db2 = DB('mongo');
     *
     * 数据库文本配置内容
     *
     *      [default]
     *      ;连接1
     *      mysql://root:123456@127.0.0.1:3306/test/?charset=utf8
     *
     *      [mongo]
     *      ;连接2
     *      mongo://127.0.0.1/test/?charset=utf8&cluster=[mongo]
     *
     *      [default.slave]
     *      mysql://192.168.1.3/?weight=30
     *      mysql://user1:pass1@192.168.1.5:3307/
     *      mysql://user:pass@192.168.1.4:3333/
     *
     *      [default.search]
     *      mysql://192.168.1.5/?weight=30
     *      mysql://192.168.1.6/?weight=40
     *      mysql://192.168.1.7/?weight=40
     *      mysql://192.168.1.7/?weight=50
     *
     *      [mongo.slave]
     *      mongo://192.168.1.2/test
     *      mongo://192.168.1.3/test
     *      mongo://192.168.1.4/test
     *
     * @var array
     */
    protected $config = [];

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

    protected $asTable = [];

    /**
     * 当前连接ID
     *
     * @var string
     */
    protected $connectionId;

    /**
     * 当前的连接对象
     *
     * @var mixed
     */
    protected $currentConnection = null;

    /**
     * 当前连接配置
     *
     * @var array
     */
    protected $currentConnectionConfig = [];

    /**
     * 集群类型
     *
     * @var string
     */
    protected $cluster = 'slave';

    /**
     * 所有连接
     *
     * 每个value都是一个数组, 类似:
     *
     * ```php
     *  $arr = [
     *      'id        => '',    // 例如 mysql://user:pass@localhost:3306'/
     *      'hostname' => '',
     *      'port'     => '',
     *      'socket'   => '',
     *      'username' => '',
     *      'password' => '',
     *      'database' => '',
     *      'time'     => '',
     *      'charset'  => '',
     *      'resource  => '',   // 资'源
     *  ];
     * ```
     *
     * @var array
     */
    protected $connections = [];

    /**
     * 按集群记录的ID
     *
     * @var array
     */
    protected $clusterConnectionIds = [];

    /**
     * 失败的连接
     *
     * @var array
     */
    protected $failConnections = [];

    /**
     * 当开启自动转换编码时原始数据编码
     *
     * null 表示不需要转换
     *
     * @var null
     */
    protected $convertToUtf8FromCharset = null;


    public function __construct(array $config)
    {
        $this->config = $config;

        if (!isset($this->config['connection']['hostname']) || !$this->config['connection']['hostname'])
        {
            throw new Exception('database config error, required hostname');
        }

        if ($this->defaultPort && (!isset($this->config['connection']['port']) || !$this->config['connection']['port'] > 0))
        {
            $this->config['connection']['port'] = $this->defaultPort;
        }

        if (!isset($this->config['connection']['id']))
        {
            $this->config['connection']['id'] = $this->config['type'] .'://'. ($this->config['connection']['username'] ? $this->config['connection']['username'].'@' : '').$this->config['connection']['hostname'] .':'. $this->config['connection']['port'] .'/';
        }

        if (isset($this->config['cluster']))foreach ($this->config['cluster'] as $clusterName => & $clusters)
        {
            foreach ($clusters as & $cluster)
            {
                if (is_string($cluster))
                {
                    $hostname            = $cluster;
                    $cluster             = $this->config['connection'];
                    $cluster['hostname'] = $hostname;
                }
                elseif ($this->defaultPort && (!isset($cluster['port']) || !$cluster['port'] > 0))
                {
                    $cluster['port'] = $this->defaultPort;
                }

                if (!isset($cluster['id']))
                {
                    $cluster['id'] = $this->config['type'] .'://'. ($cluster['username'] ? $cluster['username'].'@' : '') . $cluster['hostname'] .':'. $cluster['port'] .'/';
                }
            }
        }

        if (!isset($this->config['prefix']))
        {
            $this->config['prefix'] = '';
        }

        $this->convertToUtf8FromCharset = $this->config['autoConvertToUtf8'] && !in_array($this->config['charset'], ['UTF8', 'UTF8MB4', 'UTF16']) ? $this->config['dataCharset'] : null;
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
        $numParameters = [];
        foreach($inputParameters as $key => $value)
        {
            if (is_int($key))
            {
                $numParameters[$key] = $value;
            }
            else
            {
                $statement = str_replace($key, $this->quote($value), $statement);
            }
        }

        if ($numParameters)
        {
            # 用 ? 分割开
            $statementArray = explode('?', $statement);

            # 填补缺失的key，例如 $numParameters = [0 => 'a', 2 => 'b']; 缺失了 1
            foreach($statementArray as $key => $value)
            {
                if (!isset($statementArray[$key]))$statementArray[$key] = '?';
            }

            foreach($numParameters as $key => $value)
            {
                $statementArray[$key] = $this->quote($value) . $statementArray[$key];
            }

            # 拼接
            $statement = implode('', $statementArray);
        }

        return $this->query($statement, $asObject, $connectionType);
    }

    /**
     * 获取当前连接
     *
     * @param string $clusterName 集群名称（例如 slave, master）, 不设置则使用当前设置
     * @return mixed
     */
    public function connection($clusterName = null)
    {
        if (true === $clusterName)
        {
            $clusterName = 'master';
        }
        elseif (null === $clusterName)
        {
            $clusterName = $this->cluster;
        }

        if ($this->connectionId && $this->cluster === $clusterName)
        {
            $id = $this->connectionId;

            goto check;
        }
        elseif (isset($this->clusterConnectionIds[$clusterName]) && $this->clusterConnectionIds[$clusterName])
        {
            $id = array_rand($this->clusterConnectionIds[$clusterName]);

            check:
            if ($this->checkConnect($id))
            {
                $this->connectionId            = $id;
                $this->cluster                 = $clusterName;
                $this->currentConnectionConfig = $this->connections[$this->connectionId];

                return $this->connections[$id]['resource'];
            }
            else
            {
                $this->connectionId            = $this->connect($clusterName);
                $this->currentConnection       = $this->connections[$this->connectionId]['resource'];
                $this->cluster                 = $clusterName;
                $this->currentConnectionConfig = $this->connections[$this->connectionId];

                return $this->currentConnection;
            }
        }
        else
        {
            $this->connectionId            = $this->connect($clusterName);
            $this->currentConnection       = $this->connections[$this->connectionId]['resource'];
            $this->cluster                 = $clusterName;
            $this->currentConnectionConfig = $this->connections[$this->connectionId];

            return $this->currentConnection;
        }
    }

    /**
     * 连接数据库
     *
     * @param string $clusterName 集群名称（例如 slave, master）, 不设置则使用当前设置
     * @return string
     */
    public function connect($clusterName = null)
    {
        if (true === $clusterName)
        {
            $clusterName = 'master';
        }
        elseif (!$clusterName)
        {
            $clusterName = $this->cluster;
        }

        $id        = null;
        $resource  = null;
        $lastError = null;
        $errorHost = [];
        $config    = [];

        while(true)
        {
            $config = $this->getRandClusterHost($errorHost, $clusterName);

            if (false === $config)
            {
                # 没有可用的服务器
                if (!$lastError)
                {
                    throw new Exception("not available {$this->config['type']} {$clusterName} server.");
                }
                break;
            }

            $id = $config['id'];

            # 已经有连接了
            if (isset($this->connections[$id]))
            {
                if ($this->checkConnect($id))
                {
                    if ($this->cluster !== $clusterName)
                    {
                        $this->clusterConnectionIds[$this->cluster][$id] = $id;
                    }

                    # 返回可用连接
                    return $id;
                }
            }

            try
            {
                $resource = $this->doConnect($config);
                break;
            }
            catch (Exception $e)
            {
                $lastError = $e;
                $errorHost[$config['id']] = $config['id'];
            }
        }

        if ($lastError)
        {
            if (INCLUDE_MYQEE_CORE && IS_DEBUG && $lastError instanceof Exception)
            {
                throw $lastError;
            }
            else
            {
                throw new Exception("connect {$this->config['type']} {$clusterName} server error.");
            }
        }

        $this->connections[$id] = $config + [
            'charset'  => $this->config['charset'],
            'resource' => $resource,
            'time'     => time(),
        ];
        $this->clusterConnectionIds[$this->cluster][$id] = $id;

        return $id;
    }

    abstract protected function doConnect(array & $config);

    /**
     * 构建SQL语句
     */
    abstract public function compile($builder, $type = 'select');

    /**
     * 查询
     *
     * @param string $sql 查询语句
     * @param bool|string $asObject 是否返回对象
     * @param string $clusterName 集群名称（例如 slave, master）, 不设置则使用当前设置
     * @return Result
     */
    abstract public function query($sql, $asObject = null, $clusterName = null);


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
     * 检查当前连接是否有效
     *
     * @param $id
     * @param int $limit 时间间隔（秒）, 0 表示一直检查
     * @return mixed
     */
    abstract protected function checkConnect($id, $limit = 5);

    /**
     * 创建一个数据库
     *
     * @param string $database
     * @param string $charset 编码，不传则使用数据库连接配置相同到编码
     * @param string $collate 整理格式
     * @return boolean
     * @throws \Exception
     */
    abstract public function createDatabase($database, $charset = null, $collate = null);

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
    abstract public function quote($value);

    /**
     * 关闭链接
     */
    abstract public function closeConnect();

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
     * 返回是否支持对象数据
     *
     * @var bool
     */
    public function isSupportObjectValue()
    {
        return false;
    }

    /**
     * 获取连接ID
     *
     * @return string
     */
    public function connectionId()
    {
        return $this->connectionId;
    }

    /**
     * 获取一个随机 HOST
     *
     * @param array $excludeHosts 排除的HOST
     * @param string $clusterName 集群类型
     * @return array|false
     */
    protected function getRandClusterHost($excludeHosts = [], $clusterName = null)
    {
        if (true === $clusterName)
        {
            $clusterName = 'master';
        }
        elseif (null === $clusterName)
        {
            $clusterName = $this->cluster;
        }

        if ($clusterName === 'master' || !isset($this->config['cluster']) || !isset($this->config['cluster'][$clusterName]))
        {
            if (isset($excludeHosts[$this->config['connection']['id']]))return false;

            return $this->config['connection'];
        }
        elseif (isset($this->config['cluster'][$clusterName]))
        {
            $max = 0;
            $tmp = [];
            foreach ($this->config['cluster'][$clusterName] as $item)
            {
                if (isset($excludeHosts[$item['id']]))
                {
                    continue;
                }

                $tmp[] = [$max, $max + $item['weight'], $item];
                $max   = $max + $item['weight'];
            }

            if (!$tmp)return false;

            # 获取一个随机数
            $rand = mt_rand(1, $max);
            foreach ($tmp as $item)
            {
                if ($rand > $item[0] && $rand <= $item[1])
                {
                    return $item;
                }
            }

            return current($tmp);
        }
        else
        {
            return false;
        }
    }

    /**
     * 释放连接
     *
     * @param null $id
     * @param \Closure|null $closeCallback
     * @return $this
     */
    public function release($id = null, \Closure $closeCallback = null)
    {
        if (null === $id)
        {
            $id = $this->connectionId;
        }

        if ($id)
        {
            foreach ($this->clusterConnectionIds as $cluster => $arr)
            {
                if (isset($arr[$id]))
                {
                    unset($this->clusterConnectionIds[$cluster][$id]);
                }
            }

            # 当前连接
            if ($this->connectionId === $id)
            {
                $this->connectionId      = null;
                $this->currentConnection = null;
            }

            if ($closeCallback)
            {
                $closeCallback($this->connections[$id]['resource']);
            }

            unset($this->connections[$id]);
        }

        return $this;
    }

    /**
     * 切换编码
     *
     * 如果开启了编码转换到utf8，则进行数据编码的转换
     *
     * @param string $value
     */
    protected function convertEncoding(& $value)
    {
        if (null !== $this->convertToUtf8FromCharset)
        {
            static $mb = null;

            if (null === $mb)
            {
                $mb = function_exists('\\mb_convert_encoding');
            }

            # 转换编码编码
            if ($mb)
            {
                $value = \mb_convert_encoding((string)$value, $this->convertToUtf8FromCharset, 'UTF-8');
            }
            else
            {
                $value = \iconv('UTF-8', $this->convertToUtf8FromCharset . '//IGNORE', (string)$value);
            }
        }
    }
}