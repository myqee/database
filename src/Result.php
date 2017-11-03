<?php
namespace MyQEE\Database;

/**
 * 数据返回对象核心类
 *
 * @author     呼吸二氧化碳 <jonwang@myqee.com>
 * @category   Database
 * @copyright  Copyright (c) 2008-2018 myqee.com
 * @license    http://www.myqee.com/license.html
 */
abstract class Result implements \Countable, \Iterator, \SeekableIterator, \ArrayAccess, \JsonSerializable
{
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
    protected $result = null;

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
     * 指定数据要的转换编码
     *
     * @var false|string
     */
    protected $dataCharset = null;

    /**
     * 指定是二进制数据的key，在自动转码时会或略相应的字段
     *
     * @var array
     */
    protected $charsetBinaryField = [];

    /**
     * 设定将会自动转为游标模式的返回集最大数目
     *
     * @var int
     */
    const AUTO_CURSOR_MODE_MAX_COUNT = 100;

    /**
     * @param      $result
     * @param      $sql
     * @param      $asObject
     * @param bool $autoConvertFromCharset
     */
    public function __construct($result, $sql, $asObject, $autoConvertFromCharset = null)
    {
        $this->result = $result;
        $this->query  = $sql;

        if (is_object($asObject))
        {
            $asObject = get_class($asObject);
        }

        $this->asObject    = $asObject;
        $this->dataCharset = $autoConvertFromCharset;

        if ($this->count() > static::AUTO_CURSOR_MODE_MAX_COUNT)
        {
            # 结果集超过 200 条记录自动采用指针模式
            $this->cursorMode();
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
            throw new \Exception('method not found in ' . get_class($this));
        }
    }

    public function __destruct()
    {
        $this->free();
    }

    /**
     * 释放资源
     *
     * @return mixed
     */
    abstract public function free();

    /**
     * 返回当前行数据，执行完内部指针会向下移动一个
     */
    abstract public function fetchAssoc();

    /**
     * 统计当前查询返回数据
     *
     * @return int
     */
    abstract protected function totalCount();

    /**
     * 返回当前返回对象
     *
     * @return mixed
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
        if ($this->currentRow !== $this->internalRow && false === $this->seek($this->currentRow))
        {
            return false;
        }
        $this->internalRow++;

        if (isset($this->data[$this->currentRow]))
        {
            return $this->data[$this->currentRow];
        }

        $data       = $this->fetchAssoc();
        $currentRow = $this->currentRow;

        # 执行完 fetchAssoc() 后内部指针会向下移动一个，所以需要+1
        $this->currentRow++;

        # 处理自动编码转换
        if (null !== $this->dataCharset)
        {
            $this->_convertCharset($data);
        }

        if (true === $this->asObject)
        {
            # 返回默认对象
            $tmp = new \stdClass();
            foreach ($data as $k => $v)
            {
                $tmp->$k = $v;
            }
            $data = $tmp;
        }
        elseif (is_string($this->asObject))
        {
            # 返回指定对象
            $class = $this->asObject;
            $data  = new $class($data);
        }

        if (false === $this->cursorMode)
        {
            $this->data[$currentRow] = $data;

            if ($this->count() === count($this->data))
            {
                # 获取所有内容后释放数据库资源
                $this->free();
            }
        }

        return $data;
    }

    /**
     * 返回一个数组拷贝
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
     * @param  string $key   column for associative keys
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
                    $rs[(string)$row->$key] = $row;
                }
            }
            else
            {
                foreach ($this as $row)
                {
                    $rs[(string)$row[$key]] = $row;
                }
            }
        }
        else
        {
            if ($this->asObject)
            {
                foreach ($this as $row)
                {
                    $rs[(string)$row->$key] = $row->$value;
                }
            }
            else
            {
                foreach ($this as $row)
                {
                    $rs[(string)$row[$key]] = $row[$value];
                }
            }
        }

        $this->rewind();

        return $rs;
    }

    /**
     * 返回数组
     *
     * @param null $key
     * @param null $value
     * @return array
     */
    public function asArray($key = null, $value = null)
    {
        return $this->getArrayCopy($key, $value);
    }

    /**
     * Return the named column from the current row.
     *
     * // Get the "id" value
     * $id = $result->get('id');
     *
     * @param  string $name    column to get
     * @param  mixed  $default default value if the column does not exist
     * @return mixed
     */
    public function get($name, $default = null)
    {
        $row = $this->current();

        if ($this->asObject)
        {
            if (isset($row->$name))
            {
                return $row->$name;
            }
        }
        else
        {
            if (isset($row[$name]))
            {
                return $row[$name];
            }
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
        if (null === $this->totalRows)
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
     * 获取指定位置的数据
     *
     * @return mixed
     */
    public function offsetGet($offset)
    {
        if (isset($this->data[$offset]))
        {
            return $this->data[$offset];
        }

        if (false === $this->seek($offset))
        {
            return null;
        }

        if (!$offset != $this->currentRow)
        {
            $oldCurrent       = $this->currentRow;
            $oldInternal      = $this->internalRow;
            $this->currentRow  = $offset;
            $this->internalRow = $offset;
        }

        $rs = $this->current();

        if (isset($oldCurrent) && isset($oldInternal))
        {
            $this->currentRow  = $oldCurrent;
            $this->internalRow = $oldInternal;
        }

        return $rs;
    }

    /**
     * 设置指定位置
     *
     * [!!] 此方法禁用
     *
     * @return  void
     * @throws  \Exception
     */
    final public function offsetSet($offset, $value)
    {
        throw new \Exception('Database results are read-only');
    }

    /**
     * 删除指定位置
     *
     * [!!] 此方法禁用
     *
     * @return  void
     * @throws  \Exception
     */
    final public function offsetUnset($offset)
    {
        throw new \Exception('Database results are read-only');
    }

    /**
     * 获取当前指针位置
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
     * 移动指针到上一个
     */
    public function prev()
    {
        --$this->currentRow;
    }

    /**
     * 移动指针到下一个
     */
    public function next()
    {
        ++$this->currentRow;
    }

    /**
     * 重置指针
     *
     * !! 当开始一个 foreach 循环时，这是第一个被调用的方法。它将不会在 foreach 循环之后被调用
     */
    public function rewind()
    {
        $this->currentRow = 0;
    }

    /**
     * 验证指针是否有效
     *
     * 此方法在 Iterator::rewind() 和 Iterator::next() 方法之后被调用以此用来检查当前位置是否有效。
     *
     * @return  bool
     */
    public function valid()
    {
        return $this->offsetExists($this->currentRow);
    }

    /**
     * 对数组或字符串进行编码转换
     *
     * @param array /string $data
     */
    protected function _convertCharset(& $data)
    {
        if (is_array($data))
        {
            foreach ($data as $key => & $item)
            {
                if ($this->charsetBinaryField && isset($this->charsetBinaryField[$key]))
                {
                    continue;
                }
                $this->_convertCharset($item);
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
                $data = mb_convert_encoding($data, 'UTF-8', $this->dataCharset);
            }
            else
            {
                $data = iconv($this->dataCharset, 'UTF-8//IGNORE', $data);
            }
        }
    }

    /**
     * 设置指定的Field是二进制数据
     *
     * 此方法必须在 asArray 或 current 等前面执行
     * 当启用自动编码转换后，获取的数据会自动转码，通过此设置后可以避免对应的字段被转码
     *
     *     $this->binaryField('key1');
     *     $this->binaryField('key1' , 'key2');
     *
     * @param string $key
     * @return $this
     */
    public function binaryField($key, $key2 = null, $key3 = null)
    {
        $keys = func_get_args();
        foreach ($keys as $key)
        {
            $this->charsetBinaryField[$key] = true;
        }

        return $this;
    }

    /**
     * 支持 JSON 序列化
     *
     * @return array
     */
    public function jsonSerialize()
    {
        return $this->getArrayCopy();
    }
}
