<?php
namespace MyQEE\Database;

/**
 * 数据返回对象核心类
 *
 * @author     呼吸二氧化碳 <jonwang@myqee.com>
 * @category   Database
 * @copyright  Copyright (c) 2008-2016 myqee.com
 * @license    http://www.myqee.com/license.html
 */
abstract class Result implements \Countable, \Iterator, \SeekableIterator, \ArrayAccess
{
    /**
     * 当前配置
     *
     * @var array
     */
    protected $config;

    /**
     * 查询语句
     *
     * @var string
     */
    protected $query;

    /**
     * 返回内容的指针
     *
     * @var mixed|\mysqli_result
     */
    protected $result;

    /**
     * 返回的内容
     *
     * @var array
     */
    protected $data = [];

    /**
     * 返回总行数
     *
     * @var int
     */
    protected $totalRows;

    /**
     * 当前行数
     *
     * @var int
     */
    protected $currentRow = 0;

    /**
     * 内部指针所在行
     *
     * @var int
     */
    protected $internalRow = 0;

    protected $asObject = null;

    /**
     * 标记是否指针模式
     *
     * @var bool
     */
    protected $cursorMode = false;

    /**
     * 数据是否需要转换编码
     *
     * @var boolean
     */
    protected $charsetNeedChange = false;

    /**
     * 指定是二进制数据的key，在自动转码时会或略相应的字段
     * @var array
     */
    protected $charsetBinField = [];

    /**
     * Sets the total number of rows and stores the result locally.
     *
     * @param mixed $result query result
     * @param string $sql SQL query
     * @param $sql
     * @param $asObject
     * @param $config
     * @return void
     */
    public function __construct($result, $sql, $asObject , $config)
    {
        $this->result = $result;
        $this->query  = $sql;
        $this->config = $config;

        if (is_object($asObject))
        {
            $asObject = get_class($asObject);
        }

        $this->asObject = $asObject;

        if ($this->config['auto_change_charset'] && $this->config['charset'] !== 'UTF8')
        {
            $this->charsetNeedChange = true;
        }
        else
        {
            $this->charsetNeedChange = false;
        }
    }

    public function __call($m, $v)
    {
        if (method_exists($this->result, $m))
        {
            return call_user_func_array([$this->result, $m], $v);
        }
        else
        {
            throw new \Exception('method not found in '. get_class($this));
        }
    }

    /**
     * Result destruction cleans up all open result sets.
     *
     * @return  void
     */
    public function __destruct()
    {
        $this->releaseResource();
    }

    /**
     * 释放资源
     *
     * @return mixed
     */
    abstract protected function releaseResource();

    /**
     * 统计当前查询返回数据
     *
     * @return int
     */
    abstract protected function totalCount();

    /**
     * 返回当前行数据
     */
    abstract protected function fetchAssoc();

    /**
     * 返回当前返回对象
     */
    public function result()
    {
        return $this->result;
    }

    /**
     * 标记为指针移动模式
     *
     * 用于处理较大数据集返回，不至于出现内存超过限制的问题
     *
     * [!!] 数据库驱动是PDO时请注意，由于PDO的限制，只能指针按顺序向下移动，否则数据返回异常
     *
     * @return $this
     */
    public function cursorMode()
    {
        $this->cursorMode = true;

        return $this;
    }

    /**
     * 获取当前行数据
     *
     * @see Iterator::current()
     */
    public function current()
    {
        if ($this->currentRow !== $this->internalRow && !$this->seek($this->currentRow))return false;

        $this->internalRow ++;

        if ($this->data && array_key_exists($this->currentRow, $this->data))
        {
            return $this->data[$this->currentRow];
        }

        $data = $this->fetchAssoc();

        if ($this->charsetNeedChange)
        {
            $this->_changeDataCharset($data);
        }

        if (true === $this->asObject)
        {
            # 返回默认对象
            $data = new \stdClass($data);
        }
        elseif (is_string($this->asObject))
        {
            # 返回指定对象
            $data = new $this->asObject($data);
        }

        if (!$this->cursorMode)
        {
            $this->data[$this->currentRow] = $data;

            if ($this->count() === count($this->data))
            {
                # 释放资源
                $this->releaseResource();
            }
        }

        return $data;
    }

    /**
     * Return all of the rows in the result as an array.
     *
     * // Indexed array of all rows
     * $rows = $result->getArrayCopy();
     *
     * // Associative array of rows by "id"
     * $rows = $result->getArrayCopy('id');
     *
     * // Associative array of rows, "id" => "name"
     * $rows = $result->getArrayCopy('id', 'name');
     *
     * @param  string $key column for associative keys
     * @param  string $value column for values
     * @return array
     */
    public function getArrayCopy($key = null, $value = null)
    {
        $rs = [];

        if (null === $key && null === $value)
        {
            foreach ($this as $row)
            {
                $rs[] = $row;
            }
        }
        elseif (null === $key)
        {
            if ($this->asObject)
            {
                foreach ($this as $row)
                {
                    $rs[] = $row->$value;
                }
            }
            else
            {
                foreach ($this as $row)
                {
                    $rs[] = $row[$value];
                }
            }
        }
        elseif (null === $value)
        {
            if ($this->asObject)
            {
                foreach ($this as $row)
                {
                    $rs[$row->$key] = $row;
                }
            }
            else
            {
                foreach ($this as $row)
                {
                    $rs[$row[$key]] = $row;
                }
            }
        }
        else
        {
            if ($this->asObject)
            {
                foreach ($this as $row)
                {
                    $rs[$row->$key] = $row->$value;
                }
            }
            else
            {
                foreach ($this as $row)
                {
                    $rs[$row[$key]] = $row[$value];
                }
            }
        }

        $this->rewind();

        return $rs;
    }

    public function asArray()
    {
        return $this->getArrayCopy();
    }

    /**
     * Return the named column from the current row.
     *
     * // Get the "id" value
     * $id = $result->get('id');
     *
     * @param  string $name column to get
     * @param  mixed  $default default value if the column does not exist
     * @return mixed
     */
    public function get($name, $default = null)
    {
        $row = $this->current();

        if ($this->asObject)
        {
            if (isset($row->$name))return $row->$name;
        }
        else
        {
            if (isset($row[$name]))return $row[$name];
        }

        return $default;
    }

    /**
     * Implements [Countable::count], returns the total number of rows.
     *
     * echo count($result);
     *
     * @return  integer
     */
    public function count()
    {
        if (null===$this->totalRows)
        {
            $this->totalRows = $this->totalCount();
        }
        return $this->totalRows;
    }

    /**
     * Implements [ArrayAccess::offsetExists], determines if row exists.
     *
     *      if (isset($result[10]))
     *      {
     *          // Row 10 exists
     *      }
     *
     * @return  boolean
     */
    public function offsetExists($offset)
    {
        return ($offset >= 0 && $offset < $this->count());
    }

    /**
     * Implements [ArrayAccess::offsetGet], gets a given row.
     *
     *      $row = $result[10];
     *
     * @return  mixed
     */
    public function offsetGet($offset)
    {
        if ($this->data && array_key_exists($offset, $this->data))return $this->data[$offset];

        if (!$this->seek($offset)) return null;

        if (!$offset != $this->currentRow)
        {
            $old_current       = $this->currentRow;
            $old_internal      = $this->internalRow;
            $this->currentRow  = $offset;
            $this->internalRow = $offset;
        }

        $rs = $this->current();

        if (isset($old_current) && isset($old_internal))
        {
            $this->currentRow  = $old_current;
            $this->internalRow = $old_internal;
        }

        return $rs;
    }

    /**
     * Implements [ArrayAccess::offsetSet], throws an error.
     *
     * [!!] You cannot modify a database result.
     *
     * @return  void
     * @throws  \Exception
     */
    final public function offsetSet($offset, $value)
    {
        throw new \Exception('Database results are read-only');
    }

    /**
     * Implements [ArrayAccess::offsetUnset], throws an error.
     *
     * [!!] You cannot modify a database result.
     *
     * @return  void
     * @throws  \Exception
     */
    final public function offsetUnset($offset)
    {
        throw new \Exception('Database results are read-only');
    }

    /**
     * Implements [Iterator::key], returns the current row number.
     *
     *      echo key($result);
     *
     * @return  integer
     */
    public function key()
    {
        return $this->currentRow;
    }

    /**
     * Implements [Iterator::next], moves to the next row.
     *
     *      next($result);
     *
     * @return  $this
     */
    public function next()
    {
        ++$this->currentRow;

        return $this;
    }

    /**
     * Implements [Iterator::prev], moves to the previous row.
     *
     *      prev($result);
     *
     * @return  $this
     */
    public function prev()
    {
        --$this->currentRow;

        return $this;
    }

    /**
     * Implements [Iterator::rewind], sets the current row to zero.
     *
     *      rewind($result);
     *
     * @return  $this
     */
    public function rewind()
    {
        $this->currentRow = 0;

        return $this;
    }

    /**
     * Implements [Iterator::valid], checks if the current row exists.
     *
     * [!!] This method is only used internally.
     *
     * @return  boolean
     */
    public function valid()
    {
        return $this->offsetExists($this->currentRow);
    }

    public function fetch_array()
    {
        $data = $this->current();
        $this->next();

        return $data;
    }

    /**
     * 对数组或字符串进行编码转换
     *
     * @param array/string $data
     */
    protected function _changeDataCharset(& $data)
    {
        if (is_array($data))
        {
            foreach ($data as $key => &$item)
            {
                if ($this->charsetBinField && isset($this->charsetBinField[$key]))
                {
                    continue;
                }
                $this->_changeDataCharset($item);
            }
        }
        else
        {
            static $mb = null;
            if (null === $mb)
            {
                $mb = function_exists('\\mb_convert_encoding');
            }

            if ($mb)
            {
                $data = mb_convert_encoding($data, 'UTF-8', $this->config['data_charset']);
            }
            else
            {
                $data = iconv($this->config['data_charset'], 'UTF-8//IGNORE', $data);
            }
        }
    }

    /**
     * 设置指定的key是二进制数据
     *
     * 此方法必须在as_array或current等前面执行
     * 当启用自动编码转换后，获取的数据会自动转码，通过此设置后可以避免对应的字段被转码
     *
     *     $this->is_bin('key1');
     *     $this->is_bin('key1' , 'key2');
     *
     * @param string $key
     * @return $this
     */
    public function is_bin($key)
    {
        $keys = func_get_args();
        foreach ($keys as $key)
        {
            $this->charsetBinField[$key] = true;
        }
        return $this;
    }
}