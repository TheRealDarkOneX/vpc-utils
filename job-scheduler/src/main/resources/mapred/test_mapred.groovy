package mapred

log.debug("eval phase: ${this}")

total = 0

/**
 * @author Jon Brisbin <jon@jbrisbin.com>
 */
map = { key, values ->
  onKeyChange = {
    log.debug("map/key change: ${key}")
  }
  log.debug("map phase: ${key}=${values}")
  sale = (values.gross_price.toFloat() + values.tax.toFloat()) - values.discounts.toFloat()
  emit(message.id, key, ["total": sale])
}

reduce = { key, value ->
  onKeyChange = {
    log.debug("reduce/key change: ${key}")
    reply(["key": key, "total": total])
  }
  total += value.total.toFloat()
  log.debug("reduce phase: ${total}")
}

rereduce = { id, values ->
  log.debug("rereduce phase: ${id}=${values}")
  values.each {
    total += it
  }
  total
}
