package main

import (
	"github.com/boltdb/bolt"
	"fmt"
	"os"
	"strconv"
	"bytes"
	"encoding/json"
	"github.com/gorilla/mux"
	"net/http"
	"time"
)

type User struct {
	ID uint64
}

var kDatabase *bolt.DB

const tableName = "table1"

func main() {
	createDatabase()
	defer kDatabase.Close()
	mux := makeMuxRouter()
	httpAddr := "8080"
	//服务配置
	s := &http.Server{
		Addr:           ":" + httpAddr,
		Handler:        mux,
		ReadTimeout:    10 * time.Second,
		WriteTimeout:   10 * time.Second,
		MaxHeaderBytes: 1 << 20,
	}
	//启动服务
	if err := s.ListenAndServe(); err != nil {
		fmt.Println(err)
	}
}

//get,post 请求处理
func makeMuxRouter() http.Handler {
	//负责 get, post的请求的处理
	muxRouter := mux.NewRouter()
	muxRouter.HandleFunc("/", BackupHandleFunc).Methods("GET")
	return muxRouter
}

//创建/打开数据库
func createDatabase() {
	//Open
	//path路径下数据库存在则打开数据库，不存在创建则创建数据库
	//mode:设置文件操作权限，os.ModePerm设置为可读可写
	//options:其他配置信息,nil表示使用Bolt默认配置
	d, err := bolt.Open("zhq.db", os.ModePerm, nil)
	kDatabase = d
	if err != nil {
		fmt.Println(err)
		fmt.Println("数据库打开失败")
	}
	fmt.Println("数据库已打开")
	//defer db.Close()
}
func deleteDataWithKey(key string) {
	kDatabase.Update(func(tx *bolt.Tx) error {
		table, err := tx.CreateBucketIfNotExists([]byte(tableName))
		if err != nil {
			fmt.Println(err)
		}
		table.Delete([]byte(key))
		return nil
	})
}

//删除表
func deleteTableWithName(tName string) {
	kDatabase.Update(func(tx *bolt.Tx) error {
		err := tx.DeleteBucket([]byte(tName))
		if err != nil {
			fmt.Println(err)
		}
		return nil
	})
}

//创建/打开表
func createTable() {
	//更新数据库
	kDatabase.Update(func(tx *bolt.Tx) error {
		//如果表不存在则创建
		_, err := tx.CreateBucketIfNotExists([]byte(tableName))
		if err != nil {
			fmt.Println(err)
			fmt.Println("打开表失败")
		} else {
			fmt.Println("打开表成功")
		}
		return nil
	})
}

//向表内写入数据
func addData(key string, value string) {
	kDatabase.Update(func(tx *bolt.Tx) error {
		//如果表不存在则创建
		table, err := tx.CreateBucketIfNotExists([]byte(tableName))
		if err != nil {
			fmt.Println(err)
			fmt.Println("打开表失败")
		} else {
			fmt.Println("打开表成功")
			//写入数据
			err := table.Put([]byte(key), []byte(value))
			if err != nil {
				fmt.Println("数据写入失败")
			} else {
				fmt.Println("数据写入成功")
			}
		}
		return nil
	})
}

//
//查询数据
func fetchData() {
	kDatabase.View(func(tx *bolt.Tx) error {
		table := tx.Bucket([]byte(tableName))
		//写入数据
		result := table.Get([]byte("key"))
		fmt.Println(string(result))
		return nil
	})
}

////向表内写入数据
//func addDatawithData(data string) {
//	if data == "5" {
//		//fmt.Println("关闭数据库")
//		//kDatabase.Close()
//		panic("关闭数据库")
//	}
//	kDatabase.Update(func(tx *bolt.Tx) error {
//		//如果表不存在则创建
//		table, err := tx.CreateBucketIfNotExists([]byte(tableName))
//		if err != nil {
//			fmt.Println(data,err)
//			//fmt.Println("打开表失败")
//		} else {
//			//fmt.Println("打开表成功")
//			//写入数据
//			err = table.Put([]byte("key"), []byte(data))
//			if err != nil {
//				fmt.Println(data,":数据写入失败")
//			} else {
//				fmt.Println(data,":数据写入成功")
//			}
//		}
//		return nil
//	})
//}
func batchTest() {
	//go func() {
	//	for i := 0; i < 100; i++ {
	//		addDatawithData(strconv.Itoa(i))
	//	}
	//}()
	err := kDatabase.Batch(func(tx *bolt.Tx) error {
		go func() {
			//flag := 0
			tx2, err := kDatabase.Begin(true)
			if err != nil {
				fmt.Println(err)
			} else {
				for i := 0; i < 100; i++ {
					data := strconv.Itoa(i)
					//如果表不存在则创建
					table, err := tx2.CreateBucketIfNotExists([]byte(tableName))
					if err != nil {
						fmt.Println("setValue:", data, err)
						//fmt.Println("打开表失败")
					} else {
						//fmt.Println("打开表成功")
						//写入数据
						err = table.Put([]byte("key"), []byte(data))
						if err != nil {
							fmt.Println(data, ":数据写入失败")
						} else {
							fmt.Println(data, ":数据写入成功")
						}
					}
					//flag = i+1
				}
			}
			tx2.Rollback()

		}()
		return nil
	})
	if err != nil {
		fmt.Println("batch:", err)
	}
}

//
//获取表数据总数
func autoincrementingTest() {
	kDatabase.Update(func(tx *bolt.Tx) error {
		table := tx.Bucket([]byte(tableName))
		fmt.Println(table.NextSequence())
		return nil
	})
}

//遍历表
func fetchAllData() {
	kDatabase.View(func(tx *bolt.Tx) error {
		// Assume bucket exists and has keys
		b := tx.Bucket([]byte(tableName))

		b.ForEach(func(k, v []byte) error {
			fmt.Printf("key=%s, value=%s\n", k, v)
			return nil
		})
		return nil
	})
}
func iterating() {
	kDatabase.View(func(tx *bolt.Tx) error {
		// Assume bucket exists and has keys
		b := tx.Bucket([]byte(tableName))

		c := b.Cursor()

		for k, v := c.First(); k != nil; k, v = c.Next() {
			fmt.Printf("key=%s, value=%s\n", k, v)
		}

		return nil
	})
}

//前缀查询
func fetchAllDataWithPrefix(prefixStr string) {
	kDatabase.View(func(tx *bolt.Tx) error {
		// Assume bucket exists and has keys
		c := tx.Bucket([]byte(tableName)).Cursor()
		prefix := []byte(prefixStr)
		for k, v := c.Seek(prefix); k != nil && bytes.HasPrefix(k, prefix); k, v = c.Next() {
			fmt.Printf("key=%s, value=%s\n", k, v)
		}
		return nil
	})
}

//范围查询
func fetchAllDataWithRange(min string, max string) {
	kDatabase.View(func(tx *bolt.Tx) error {
		// Assume our events bucket exists and has RFC3339 encoded time keys.
		c := tx.Bucket([]byte(tableName)).Cursor()

		// Our time range spans the 90's decade.
		minByte := []byte(min)
		maxByte := []byte(max)

		// Iterate over the 90's.
		for k, v := c.Seek(minByte); k != nil && bytes.Compare(k, maxByte) <= 0; k, v = c.Next() {
			fmt.Printf("%s: %s\n", k, v)
		}
		return nil
	})
}

//表嵌套
func createUser(accountID uint64, u *User) error {
	//开启事务
	tx, err := kDatabase.Begin(true)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	//打开Account表
	root, _ := tx.CreateBucketIfNotExists([]byte(strconv.FormatUint(accountID, 10)))

	//在Account表中嵌套USRS表
	bkt, err := root.CreateBucketIfNotExists([]byte("USERS"))
	if err != nil {
		return err
	}

	//设置userID为USERS表的自增量的值
	userID, err := bkt.NextSequence()
	if err != nil {
		return err
	}
	u.ID = userID

	// 将user结构体转byte数组
	if buf, err := json.Marshal(u); err != nil {
		return err
	} else if err := bkt.Put([]byte(strconv.FormatUint(u.ID, 10)), buf); err != nil {
		return err
	}

	// 提交事务
	if err := tx.Commit(); err != nil {
		return err
	}

	return nil
}
func fetchAccout() {
	kDatabase.View(func(tx *bolt.Tx) error {
		// Assume bucket exists and has keys
		b := tx.Bucket([]byte([]byte(strconv.FormatUint(1234, 10))))

		b.ForEach(func(k, v []byte) error {
			fmt.Printf("key=%s, value=%s\n", k, v)
			//fetchUser(k)
			return nil
		})
		return nil
	})
}
func fetchUser() {
	kDatabase.View(func(tx *bolt.Tx) error {
		// 1、打开account表
		table := tx.Bucket([]byte([]byte(strconv.FormatUint(1234, 10))))
		//2、打开嵌套在account表中的USERS表
		userTable := table.Bucket([]byte("USERS"))
		//3、遍历USERS表
		userTable.ForEach(func(k, v []byte) error {
			fmt.Printf("user: key=%s, value=%s\n", k, v)
			return nil
		})
		return nil
	})
}

//备份数据库
func BackupHandleFunc(w http.ResponseWriter, req *http.Request) {
	err := kDatabase.View(func(tx *bolt.Tx) error {
		w.Header().Set("Content-Type", "application/octet-stream")
		w.Header().Set("Content-Disposition", `attachment; filename="zhq.db"`)
		w.Header().Set("Content-Length", strconv.Itoa(int(tx.Size())))
		_, err := tx.WriteTo(w)
		return err
	})
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
}
func BackuptoFile() {
	err := kDatabase.View(func(tx *bolt.Tx) error {
		tx.CopyFile("zhq2.db", os.ModePerm)
		return nil
	})
	if err != nil {
		fmt.Println(err)
	}
}
