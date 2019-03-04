//$$ TODO
package com.example.demo

import org.slf4j.LoggerFactory
import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.boot.runApplication
import org.springframework.data.jpa.repository.JpaSpecificationExecutor
import org.springframework.data.jpa.repository.Query
import org.springframework.data.repository.CrudRepository
import org.springframework.data.repository.query.Param
import java.io.Serializable
import java.util.concurrent.atomic.AtomicInteger
import javax.persistence.Column
import javax.persistence.Entity
import javax.persistence.EntityManager
import javax.persistence.Id
import javax.persistence.LockModeType
import javax.persistence.PersistenceContext
import javax.persistence.Version
import javax.transaction.Transactional

val log = LoggerFactory.getLogger("dktest")
val AppStart = System.currentTimeMillis()

@Entity
class TB() : Serializable {
    constructor(_uid: Int, _money: Int) : this() {
        this.uid = _uid
        this.money = _money
    }

    @Id
    @Column(updatable = false, nullable = false)
    var uid: Int? = null

    @Column(nullable = false)
    var money: Int = 0

    @Column(updatable = false, nullable = false)
    var runKey: Long = com.example.demo.AppStart % 10000

    @Version
    @Column(name = "OBJ_VERSION", nullable = false)
    var version: Int? = 0

}


data class MoneyTransfer(val _tid: Int, val from: Int, val to: Int, val v: Int = 1,
                         val _id: Int = idgen.getAndIncrement()) {
    companion object {
        private val idgen = AtomicInteger(0)
    }
}

interface MyRepo : CrudRepository<TB, Int>, MyRepoCustom, JpaSpecificationExecutor<TB> {

    @Query(value = "select * from TB where uid=:uid for update", nativeQuery = true)
    fun xlockOneNative(@Param("uid") uid: Int): TB

}


interface MyRepoCustom {
    fun xlockOneForUpdate(uid: Int): TB

    @Transactional
    fun transferPessimisticV1(tf: MoneyTransfer)

    @Transactional
    fun initDB(m: Int, mrepo: MyRepo)


    fun evicCache()

    @Transactional
    fun transferOptimisticV1(tf: MoneyTransfer)
}


open class MyRepoCustomImpl : MyRepoCustom {
    override fun evicCache() {
        em.entityManagerFactory.cache.evictAll()

    }

    @PersistenceContext
    lateinit var em: EntityManager

    @Transactional
    override fun initDB(M: Int, mrepo: MyRepo) {
        mrepo.deleteAll()

        (1..M).forEach { i ->
            val tb = TB(i, 1_000_000)
            mrepo.save(tb)
        }


    }

    override fun xlockOneForUpdate(uid: Int): TB {
        val tb = em.find(TB::class.java, uid, LockModeType.PESSIMISTIC_WRITE)!!
        return tb
    }

    @Transactional
    override fun transferOptimisticV1(tf: MoneyTransfer) {

        val fobj = em.find(TB::class.java, tf.from)
        val tobj = em.find(TB::class.java, tf.to)

        fobj.money -= tf.v
        tobj.money += tf.v

        em.persist(fobj)
        em.persist(tobj)


    }


    @Transactional
    override fun transferPessimisticV1(tf: MoneyTransfer) {
        val fobj: TB
        val tobj: TB
        if (tf.from <= tf.to) {
            fobj = xlockOneForUpdate(tf.from)
            tobj = xlockOneForUpdate(tf.to)
        } else {
            tobj = xlockOneForUpdate(tf.to)
            fobj = xlockOneForUpdate(tf.from)
        }

        fobj.money -= tf.v
        tobj.money += tf.v

        em.persist(fobj)
        em.persist(tobj)

    }

}


@SpringBootApplication
class DemoApplication {
}

fun main(args: Array<String>) {
    println("empty")
    runApplication<DemoApplication>(*args)
}