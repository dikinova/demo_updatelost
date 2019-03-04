//$$ TODO
package com.example.demo

import org.junit.Test
import org.junit.runner.RunWith
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.dao.ConcurrencyFailureException
import org.springframework.test.context.junit4.SpringRunner
import java.util.*
import kotlin.concurrent.thread

enum class LockModeType { Optimistic, Pessimistic }


private val CN = 30
private val M = 30
val LockMode = LockModeType.Optimistic


@RunWith(SpringRunner::class)
@SpringBootTest
class DemoApplicationTests {

    @Autowired
    lateinit var mrepo: MyRepo

    val random = Random(System.currentTimeMillis())

    private fun jobs(tid: Int): List<MoneyTransfer> {
        val js = (1..M).flatMap { from ->
            (1..M).map { to -> MoneyTransfer(tid, from, to) }
        }.filter { it.from != it.to }
        return js
    }

    private fun systemExit(s: String) {
        log.error("exit system: {}", s)
        Thread.sleep(100)
        System.exit(-1)
    }

    @Test
    fun test001() {
        log.warn("test001 START {}", AppStart)
        mrepo.initDB(M, mrepo)

        val lockMode = LockMode

        val ts = (1..CN).map { tid ->
            thread(start = false, name = "th$tid") {
                var tfc = 0
                fun clearCache() {
                    tfc += 1
                    if (tfc % 100 == 50) mrepo.evicCache()
                }

                jobs(tid).shuffled().forEach { transfer ->
                    var stopLoop = false
                    while (!stopLoop) {
                        Thread.sleep(random.nextInt(CN * 3 * 2).toLong())

                        when (lockMode) {
                            LockModeType.Optimistic -> {

                                try {
                                    mrepo.transferOptimisticV1(transfer)
                                    clearCache()
                                    log.info("ok {}", transfer)
                                    stopLoop = true
                                } catch (e: Exception) {
                                    log.info("${e.javaClass}: ${e.message}")
                                    if (e !is ConcurrencyFailureException) {
                                        systemExit("${e.javaClass}: ${e.message}")
                                    }
                                }
                            }

                            LockModeType.Pessimistic -> {
                                try {
                                    mrepo.transferPessimisticV1(transfer)
                                    clearCache()
                                    log.info("ok {}", transfer)
                                    stopLoop = true
                                } catch (e: Exception) {
                                    systemExit("${e.javaClass}: ${e.message}")
                                }
                            }

                        }


                    }

                }
            }
        }

        ts.forEach { it.start() }
        ts.forEach { it.join() }

        val MVER = (M - 1) * 2 * CN // target version = (M-1)*2*CN

        log.warn("END maxv:{} tfs:{}", MVER, M * (M - 1) * CN)

        val tbs = (1..M).map { mrepo.findById(it).get() }
        if (tbs.all { it.money == 1_000_000 }) {
            log.warn("ALLOK {} time:{}", AppStart, (System.currentTimeMillis() - AppStart) / 1000)
        } else {
            val err = "money error:" + tbs.map { it.money }
            log.error(err)
            throw RuntimeException(err)
        }

    }

}
