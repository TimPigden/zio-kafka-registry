package zio.kafka.registry.rest

/**
 * urls for rest interface (here to allow easy separation of rest clients)
 */
object Urls {

  def schema(id: Int): String = s"/schemas/ids/$id"

  def subjects: String = s"/subjects"

  def subjectVersions(subject: String): String = s"/subjects/$subject/versions"

  def deleteSubject(subject: String): String = s"/subjects/$subject"

  def version(subject: String, versionId: Int): String = s"/subjects/$subject/versions/$versionId"

  def schema(subject: String, versionId: Int): String = s"/subjects/$subject/versions/$versionId/schema"

  def postSchema(subject: String): String = s"/subjects/$subject/versions"

  def alreadyPresent(subject: String): String = s"s/subjects/$subject"

  def delete(subject: String, versionId: Int): String = s"/subjects/$subject/versions/$versionId"

  def compatible(subject: String, versionId: Int): String = s"/compatibility/subjects/$subject/versions/$versionId"

  def setConfig: String = s"/config"

  def config: String = s"/config"

  def setConfig(subject: String): String = s"/config/$subject"

  def config(subject: String): String = s"/config/$subject"
}
