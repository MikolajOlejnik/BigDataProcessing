// Necessary package members

import akka.actor.{Actor, ActorRef, ActorSystem, Props}
import akka.pattern.ask
import akka.util.Timeout

import scala.collection.mutable.ArrayBuffer
import scala.concurrent.duration._
import scala.concurrent.Await
import scala.io.{BufferedSource, Source}

// Actor to send individual counts to, and to group together results from child actors
class MainActor extends Actor {

  // Reference to send result to
  var sendResultTo : ActorRef = context.self
  // listsReceived & numActors are used to determine by the main actor when all counts have been received
  var listsReceived = 0
  var numActors = 0

  // ArrayBuffer of character-count pairs send by child actors
  // ArrayBuffer is used instead of Array as we don't know what the required size of the array would be
  /* Whilst it would be safe to assume that an Array of size numActors * 26 (num of characters in alphabet),
  * this solution wouldn't take into account alphabets with more/less letters (e.g. French), also there is
  * no guarantee that each of the letters is present in the chunk allocated to each actor, hence an
  * ArrayBuffer is resizeable, whilst Array creates a new Arrays which wouldn't work given the scope
  * of my program */

  var countedLists = ArrayBuffer[List[(Char, Int)]]()

  // Runs when message is received
  def receive : Receive = {

    // If BufferedSource was sent, that means we received a raw file
    case f : BufferedSource =>
      // Flatten file into a list of letters/numbers/symbols, then keep only the letters and make them lowercase
      val sanitisedChars = f.getLines.flatten.filter(_.isLetter).toList.map(x => x.toLower)

      // Determine number of actors based on character number, a new actor will be created every 100,000 characters
      // Rounded up in case the file doesn't have 100,000 characters
      numActors = math.ceil(sanitisedChars.length.toDouble / 100000).toInt

      // Remember who sent the input list
      sendResultTo = sender()

      // Number of characters each actor will sort
      val rangeSize = sanitisedChars.length / numActors
      // Initialising range of indexes first actor will take as input
      var lowerLimit = 0
      var upperLimit = rangeSize

      // Create child actors with segment of list
      for (count <- 1 to numActors) {
        /* Create actor, send their portion of file to sort as message
        Not instantiated with the input list to make it easier to visualise what is happening */
        val aCountingActor = context.actorOf(Props[CountingActor])
        aCountingActor ! sanitisedChars.slice(lowerLimit, upperLimit)
        // Increment indexes for next actor
        lowerLimit = upperLimit
        upperLimit += rangeSize
      }

    // Results received from in child actors: List of lists of form (letter, occurrences)
    case f : List[(Char, Int)] =>

      // Keep track of results received
      listsReceived += 1
      // Append lists to ArrayBuffer of all results received
      countedLists += f

      // All results received
      if (listsReceived == numActors) {
        // Combine all results from ArrayBuffer into 1 list
        val combinedLists = countedLists.toList.flatten
        // Group by character
        // Then for each key value pair, sum together all the values for the given key (i.e. letter)
        val grouped = combinedLists.groupBy(_._1).map { case (k, v) => k -> v.map { _._2}.sum}
        // Send final result
        sendResultTo ! grouped.toList
      }

    // Wildcard, something went wrong
    case _ =>
      sendResultTo ! "Unexpected result"
  }
}

// CountingActor class groups characters together, counts them and send them off to the parent
class CountingActor extends Actor {


  def receive: Receive = {

    // Everything received should be a list of characters
    case inputList : List[Char] =>

      // Put like characters together into a list
      val charGroup = inputList.groupBy(x => x)
      // Count all the characters for each given key
      val charCount = charGroup.map(x => (x._1, x._2.length)).toList

      context.parent ! charCount

    case _ =>
      context.parent ! "No list received!"
  }
}

// Program entry point
object Main extends App {

  // Create new ActorSystem, and instantiate the main actor of this actor system of type actor
  val s = ActorSystem("actor-system")
  val mainActor = s.actorOf(Props[MainActor])
  val timeout = Timeout (FiniteDuration(Duration("15 seconds").toSeconds, SECONDS))
  // Give the mainActor bleak-house.txt as input, which will wait 15 seconds to receive a response
  val future = ask (mainActor, Source.fromFile("bleak-house.txt")) (timeout)
  val result = Await.result (future, timeout.duration)
  println ("List of occurrences: " + result)
  // Close ActorSystem
  s.terminate()

}
