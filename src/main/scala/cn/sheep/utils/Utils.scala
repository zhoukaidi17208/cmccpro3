package cn.sheep.utils

import org.apache.commons.lang3.time.FastDateFormat

object Utils {

    private val format: FastDateFormat = FastDateFormat.getInstance("yyyyMMddHHmmssSSS")
    def caculateRqt(startTime: String, endTime: String): Long = {


        val start = startTime.substring(0, 17)
        format.parse(endTime).getTime - format.parse(start).getTime
    }

}
