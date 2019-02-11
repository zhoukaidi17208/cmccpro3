package cmcc.utils

import org.apache.commons.lang3.time.FastDateFormat


object CaculateTools {
  private val format: FastDateFormat = FastDateFormat.getInstance("yyyyMMddHHmmssSSS")

  /**
    * 计算时间差
    */
  def caculateTime(startTime: String, endTime: String): Long = {

    val start = startTime.substring(0, 17)
    format.parse(endTime).getTime - format.parse(startTime).getTime



  }
}