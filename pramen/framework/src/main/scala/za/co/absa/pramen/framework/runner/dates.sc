import java.time.LocalDate
import java.time.format.DateTimeFormatter

val format = "yyyy-MM"

val fmt = DateTimeFormatter.ofPattern(format)

LocalDate.now.format(fmt)

fmt.getClass.toString


