# 迈启数据库类库

迈启数据库类库是一个功能强大的数据库类库，可以独立使用，支持自动主从读写分离、支持 MySQL、MongoDB 等常用数据库，除了可以避免数据注入，还可以让你几乎不用写SQL语句就可以实现对数据库的查询、操作等，而且做到了兼容大部分数据库语法的功能，这样你即便需要把MySQL换成MongoDB都不需要改动什么代码，大大增加代码的可移植性。DB 类继承了 [QueryBuilder](guide/zh-cn/querybuilder.md) 类，可以实现大部分语句的跨数据库类型的解析。

一个简单的例子：

```php
$db = DB::instance();
$name = "test'abc";       // 注意这个字符串带单引号'的
$rs = $db->from('mytable')->orderBy('id', 'DESC')->where('id', 10, '>')->limit(100)->where('name', $name)->get();
foreach($rs as $item)
{
    print_r($item);
}
```

这个在 MySQL 的驱动里会自动生成这样的SQL语句：

```sql
SELECT * FROM `mytable` WHERE `id` > '10' AND `name` = 'test\'abc' ORDER BY `id` DESC LIMIT 100;
```

如果是 MongoDB 的话，则会使用 MongoDB 的语法进行查询，类似：

```
db.mytable.find({"id": {"$gt": 10}, "name": 'test\'abc'}).sort({"id": -1}).limit(100)
```


## DB::instance($configName = 'default')

获取一个静态已实例化的对象，`Database::instance()` 和 `new Database()` 都会返回一个实例化好的Database对象，不同的是，前者不会重复构造，而后者每次都会实例化，推荐使用`Database::instance()`

```php
use \MyQEE\Database\DB;
    
$db1 = DB::instance();
$db2 = DB::instance();
$db3 = DB::instance('test');
$db4 = DB::instance('test');
$db5 = new DB();
    
var_dump($db1 === $db2);   //bool(true) 
var_dump($db3 === $db4);   //bool(true) 
var_dump($db1 === $db3);   //bool(false) 
var_dump($db1 === $db5);   //bool(false) 
```

## $db->get($asObject = false, $useMaster = null)

构造SQL并进行查询，它首先执行compile()方法获得SQL语句，然后用此SQL语句执行query()，是数据库对象中比较常用的一个方法

```php
use \MyQEE\Database\DB;
    
$db = DB::instance();
$arr = $db->from('mytable')->where('id', 1)->get()->asArray();
    
$db->where('id',1);
$db->from('mytable');
$rs = $db->get();
echo $rs->count();  // 行数
foreach ($rs as $item)
{
    ...
}
```    

## $db->query($sql, $asObject = false, $useMaster = null)

执行SQL，由于自行拼写的SQL语句会存在一定的安全隐患，所以推荐尽量少用此方法直接执行SQL语句

* $sql - 需要执行的SQL语句
* $as_object - 返回对象，true - 返回一个stdClass ， 或者指定一个其它的对象名，比如Arr
* $use_master - 是否使用主数据库查询，只对查询语句设置有效（update,insert等语句会自动切换到主数据库），当然，只有你的数据库配置里配置了主从数据库才会起到实际的作用

```
$db = DB::instance();
$arr = $db->query('SELECT * FROM `mytable` where `id` = 1')->asArray();
print_r($arr);

// 支持直接返回一个stdClass对象
$arr = $db->query('SELECT * FROM `mytable` where `id` = 1',true)->asArray();
var_dump( $arr[0] instanceof strClass );   //bool(true)
    
// 支持直接返回一个用户自定义对象
$arr = $db->query('SELECT * FROM `mytable` where `id` = 1','MyClass')->asArray();
var_dump( $arr[0] instanceof MyClass );   //bool(true)
```

## $db->lastQuery()

返回此数据库对象最后一次执行的SQL语句

```php
$db->from('mytable')->where('id', 1)->get();
echo $db->lastQuery();     //SELECT * FROM `mytable` where `id` = 1
```

## $db->update($table = null, $value = null, $where = null)

更新数据

```php
$db = DB::instance();
$data = [
    'title' => 'test',
    'count' => 1,
];
$where = [
    'id' => 1,
];
$rs = $db->update('test',$data,$where);
echo $rs;                //影响的行数，若返回的是0，则表示没有更新到数据
echo $db->lastQuery();   //UPDATE `test` SET `title` = 'test', `count` = 1 WHERE `id` = 1
    
    
// 同上
$db->where($where)->update('test',$data);
    
// 同上
$db->where($where)->set($data)->table('test')->update();
```

## $db->insert($table = null, $value = null)

插入数据，用法基本和update()一样，只是没有where条件

```php
$db = DB::instance();
$data = [
    'title' => 'test',
    'count' => 1,
];
// 若操作失败返回false，否则返回一个数组
$rs = $db->insert('test',$data);
print_r($rs)    //返回的是一个数组，例如：array(10,1);  其中10表示自增ID号，1表示作用行数
```

## $db->delete($table = null, $where = null)

删除指定条件下的数据

```php
    $db = DB::instance();
    $rs = $db->delete('test' , array('id'=>1) );
    echo $rs;   // 作用行数，0表示没有删除数据，1表示删除1行，2表示删除了2行，以此类推
```
    
## $db->countRecords($table = null, $where = null)

统计指定条件下数目

```php
$db = DB::instance();
    
// 返回 `class_id` = 1 条件下mytable表行数
echo $db->where('class_id' , 1)->countRecords('mytable');      

//SELECT COUNT(1) AS `totalRowCount` FROM `mytable` WHERE `class_id` = 1
echo $db->lastQuery();
```

## $db->replace($table = null, $value = null, $where = null)

替换数据，即MySQL的REPLACE INTO，用法同update()，返回作用行数

## merge()

replace() 方法的别名
    

## $db->tablePrefix()

返回当前数据库配置表前缀，注意，只有在使用自己定义的$sql时需要注意需要自行加上表前缀，否则使用QueryBuilder构造出的SQL时系统会自动加上表前缀
    
```php 
$db = DB::instance();
$arr = $db->query('SELECT * FROM `'.$db->table_perfix().'mytable` where `id` = 1')->asArray();

// 效果同上
$arr = $db->from('mytable')->where('id', 1)->get()->asArray();
```
    
## $db->compile($type = 'select')

构造生成SQL语句并返回

```php
$sql = $db->from('mytable')->where('id', 1)->compile();
echo $sql;      //SELECT * FROM `mytable` where `id` = '1'
    
$sql = $db->value('t',1)->table('mytable')->compile('update');
echo $sql;      //UPDATE `mytable` SET `t` = 1
```

## $db->driver()

返回当前驱动对象。目前支持MySQL和MySQLI两种类型

## $db->autoUseMaster($autoUseMaster = true)

设置是否一直在主数据库上查询
这样设置后，select会一直停留在主数据库上，直到$this->auto_use_master(false)后才会自动判断

## $db->isAutoUseMaster()

返回当前是否一直使用主数据库

## DB::parseDsn($dsn)

解析一个类似 `mysql://root:123456@localhost:3306/mydb/` 为一个数组配置格式


## $db->transaction()

返回一个数据库事务对象，若当前驱动不支持事务则返回一个 false