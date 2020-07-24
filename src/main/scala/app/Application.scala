package app

import cats.effect._
import cats.syntax.flatMap._
import cats.syntax.functor._
import fs2.Stream
import fs2.kafka._
import io.chrisdavenport.log4cats.Logger
import io.chrisdavenport.log4cats.slf4j.Slf4jLogger

import scala.concurrent.duration._

object Application extends IOApp {

  private val hostname = "127.0.0.1"
  private val port = 9092
  private val groupId = "test-group"
  private val topic = "test-topic"

  type K = String
  type V = String

  type Consumer[F[_]] = KafkaConsumer[F, K, V]

  def run(args: List[String]): IO[ExitCode] = resource[IO].use {
    case (consumer, log) =>
      stream(consumer, log).compile.drain as ExitCode.Success
  }

  private def resource[F[_] : ConcurrentEffect : ContextShift : Timer]: Resource[F, (Consumer[F], Logger[F])] =
    for {
      log      <- Resource.liftF(Slf4jLogger.create[F])
      consumer <- kafkaConsumerResource[F]
    } yield (consumer, log)

  private def kafkaConsumerResource[F[_] : ConcurrentEffect : ContextShift : Timer]: Resource[F, Consumer[F]] =
    consumerResource {
      ConsumerSettings[F, K, V]
        .withBootstrapServers(s"$hostname:$port")
        .withAutoOffsetReset(AutoOffsetReset.Latest)
        .withEnableAutoCommit(false)
        .withGroupId(groupId)
    }.evalTap(_.subscribeTo(topic))

  private def stream[F[_] : Concurrent : Timer](consumer: Consumer[F], log: Logger[F]): Stream[F, Unit] =
    consumer.stream
      .evalMap { commitable =>
        log.info("New message received") >>
          log.debug(s"key: ${commitable.record.key}") >>
          log.debug(s"value: ${commitable.record.value}") as commitable.offset
      }
      .through(commitBatchWithin[F](3, 1.second))
}
