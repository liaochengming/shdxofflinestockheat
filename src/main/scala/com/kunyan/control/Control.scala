package com.kunyan.control

import java.text.SimpleDateFormat
import java.util.regex.Pattern

import org.apache.log4j.{Level, LogManager}
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ListBuffer

/**
 * Created by lcm on 2017/3/24.
 * 主类
 */
object Control {

  val urlMatches = Array(
    // 搜索
    "quotes.money.163.com.*word=(.*)&t=", "quotes.money.163.com.*word=(.*)&count",
    "suggest3.sinajs.cn.*key=(((?!&name).)*)", "xueqiu.com.*code=(.*)&size", "xueqiu.com.*q=(.*)",
    "app.stcn.com.*&wd=(.*)", "q.ssajax.cn.*q=(.*)&type", "znsv.baidu.com.*wd=(.*)&ch",
    "so.stockstar.com.*q=(.*)&click", "quote.cfi.cn.*keyword=(.*)&his",
    "hq.cnstock.com.*q=(.*)&limit", "xinpi.cs.com.cn.*q=(.*)", "xinpi.cs.com.cn.*c=(.*)&q",
    "search.cs.com.cn.*searchword=(.*)", "d.10jqka.com.cn.*(\\d{6})/last",
    "news.10jqka.com.cn.*keyboard_(\\d{6})", "10jqka.com.cn.*w=(.*)&tid",
    "data.10jqka.com.cn.*/(\\d{6})/", "dict.hexin.cn.*pattern=(\\d{6})",
    "dict.hexin.cn.*pattern=(\\S.*)", "q.stock.sohu.com.*keyword=(.*)&(_|c)",
    "api.k.sohu.com.*words=(.*)&p1", "hq.p5w.net.*query=(.*)&i=", "code.jrjimg.cn.*key=(.*)&d=",
    "itougu.jrj.com.cn.*keyword=(.*)&is_stock", "wallstreetcn.com/search\\?q=(.*)",
    "api.markets.wallstreetcn.com.*q=(.*)&limit", "so.hexun.com.*key=(.*)&type",
    "finance.ifeng.com.*code.*(\\d{6})", "finance.ifeng.com.*q=(.*)&cb",
    "suggest.eastmoney.com.*input=(.*?)&", "so.eastmoney.com.*q=(.*)&m",
    "guba.eastmoney.com/search.aspx?t=(.*)", "www.yicai.com.*searchKeyWords=(.*)",
    "www.gw.com.cn.*q=(.*)&s=", "cailianpress.com/search.*keyword=(.*)",
    "api.cailianpress.com.*PutinInfo=(.*)&sign", "news.21cn.com.*keywords=(.*)&view",
    "news.10jqka.com.cn.*text=(.*)&jsoncallback", "smartbox.gtimg.cn.*q=(.*?)&",
    "swww.niuguwang.com/stock/.*q=(.*?)&", "info.zq88.cn:9085.*query=(\\d{6})&",

    // 查看
    "quotes.money.163.com/app/stock/\\d(\\d{6})", "m.news.so.com.*q=(\\d{6})",
    "finance.sina.com.cn.*(\\d{6})/nc.shtml", "guba.sina.com.cn.*name=.*(\\d{6})",
    "platform.*symbol=\\w\\w(\\d{6})", "xueqiu.com/S/.*(\\d{6})", "cy.stcn.com/S/.*(\\d{6})",
    "mobile.stcn.com.*secucode=(\\d{6})", "stock.quote.stockstar.com/(\\d{6}).shtml",
    "quote.cfi.cn.*searchcode=(.*)", "hqapi.gxfin.com.*code=(\\d{6}).sh",
    "irm.cnstock.com.*index/(\\d{6})", "app.cnstock.com.*k=(\\d{6})",
    "guba.eastmoney.com.*code=.*(\\d{6})", "stockpage.10jqka.com.cn/(\\d{6})/",
    "0033.*list.*(\\d{6}).*json", "q.stock.sohu.com/cn/(\\d{6})/", "s.m.sohu.com.*/(\\d{6})",
    "data.p5w.net.*code.*(\\d{6})", "hq.p5w.net.*a=.*(\\d{6})", "jrj.com.cn.*(\\d{6}).s?html",
    "mapi.jrj.com.cn.*stockcode=(\\d{6})", "api.buzz.wallstreetcn.com.*(\\d{6})&cid",
    "stockdata.stock.hexun.com/(\\d{6}).shtml", "finance.ifeng.com/app/hq/stock.*(\\d{6})",
    "api.3g.ifeng.com.*k=(\\d{6})", "quote.eastmoney.com.*(\\d{6}).html", "gw.*stock.*?(\\d{6})",
    "tfile.*(\\d{6})/fs_remind", "api.cailianpress.com.*(\\d{6})&Sing",
    "stock.caijing.com.cn.*(\\d{6}).html", "gu.qq.com.*(\\d{6})\\?", "gubaapi.*code=.*(\\d{6})",
    "mnews.gw.com.cn.*(\\d{6})/(list|gsgg|gsxw|yjbg)", "101.226.68.82.*code=.*(\\d{6})",
    "183.238.123.235.*code=(\\d{6})&", "zszx.newone.com.cn.*code=(\\d{6})",
    "compinfo.hsmdb.com.*stock_code=(\\d{6})", "mt.emoney.cn.*barid=(\\d{6})",
    "news.10jqka.com.cn/stock_mlist/(\\d{6})", "www.newone.com.cn.*code=(\\d{6})",
    "219.141.183.57.*gpdm=(\\d{6})&", "www.hczq.com.*stockCode=(\\d{6})",
    "open.hs.net.*en_prod_code=(\\d{6})", "stock.pingan.com.cn.*secucodes=(\\d{6})",
    "58.63.254.170:7710.*code=(\\d{6})&", "sjf10.westsecu.com/stock/(\\d{6})/",
    "mds.gf.com.cn.*/(\\d{6})", "211.152.53.105.*key=(\\d{6})"
  )


  def main(args: Array[String]) {

    val sparkConf = new SparkConf().setAppName("shdx_offline_stock ")
    //      .setMaster("local")

    val sc = new SparkContext(sparkConf)
    LogManager.getRootLogger.setLevel(Level.WARN)

    println("start!")
    sc.textFile(args(0))
      .map(x => getStockData(x))
      .filter(x =>{
        x != null && x._2 != "none"
      })
      .groupBy(x => (x._2, x._3, x._5))
      .flatMap(x => {
        val list = new ListBuffer[(String, String, String, String, String)]
        for (data <- x._2) {
          list.+=((data._1, data._4, data._5, data._6, x._2.size.toString))
        }
        list
      })
      //.filter(_._5.toInt<10)
      .groupBy(x => x._1)
      .flatMap(x => {
        val list = new ListBuffer[String]
        var num = 1
        for (data <- x._2) {
          list.+=(data._1 + "_" + num + "\t" + data._2 + "\t" + data._3 + "\t" + data._4 + "\t" + data._5)
          num = num + 1
        }
        list
      })
      .repartition(1)
      .saveAsTextFile(args(1))

  }


  //将url提取出host
  def getHost(url: String): String = {

    var host = url
    if (url.startsWith("http://"))
      host = url.replace("http://", "")
    if (url.startsWith("https://"))
      host = url.replace("https://", "")
    host = host.split("/")(0).split(":")(0)
    if (host.startsWith("www."))
      host = host.replace("www.", "")
    host
  }


  //获取股票热度数据
  def getStockData(data: String): (String, String, String, String, String, String) = {

    val arr = data.split("\\t")
    val ad = arr(1)
    val url = arr(3)
    val time = arr(2)
    val min = getMin(time)
    val sec = getSec(time)

    for (index <- urlMatches.indices) {

      val pattern = Pattern.compile(urlMatches(index))
      val res = pattern.matcher(url)

      if (res.find()) {
        return (sec, min, ad, time, res.group(1), index.toString)
      }
    }

    null
  }

  //获取时间精确到分钟
  def getMin(time: String): String = {
    val df = new SimpleDateFormat("yyyyMMddHHmm")
    df.format(time.toLong)
  }

  //获取时间精确到秒钟
  def getSec(time: String): String = {
    val df = new SimpleDateFormat("yyyyMMddHHmmss")
    df.format(time.toLong)
  }
}
