package zio.kafka.registry

import io.confluent.kafka.serializers.subject.{RecordNameStrategy, TopicNameStrategy, TopicRecordNameStrategy}

object Settings {

  /**
   * subject name strategy is about in which subject look for the schema
   * See confluent schema registry docs
   */
  sealed trait SubjectNameStrategy {
    def strategyClassName: String
  }

  case object TopicNameStrategy extends SubjectNameStrategy {
    override val strategyClassName: String = classOf[TopicNameStrategy].getName
  }
  case object RecordNameStrategy extends SubjectNameStrategy {
    override val strategyClassName: String = classOf[RecordNameStrategy].getName
  }
  case object TopicRecordNameStrategy extends SubjectNameStrategy {
    override val strategyClassName: String = classOf[TopicRecordNameStrategy].getName
  }

  /**
   * allows creation of a custom strategy
   * @param strategyClassName the class of the strategy interface
   */
  case class CustomStrategy(strategyClassName: String) extends SubjectNameStrategy

}
