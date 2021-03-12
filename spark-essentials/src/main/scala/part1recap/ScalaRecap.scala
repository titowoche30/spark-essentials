package part1recap
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.util.{Failure, Success}

object ScalaRecap extends App{
    val aFuture = Future {
        // some expensive computation, runs on another Thread
        42
    }
    // In order do process the value of the Future whe can use the functional methods similar to the colection
    // methods, like map,flatMap and filter.

    // Or call onComplete and then provide a pattern match on the possible value that this future
    // can be completed with.

    // The future can be complete with a Success(value) containing the value or a Failure containing some exception
    // that might have be throwned on that thread

    aFuture.onComplete {
        case Success(a) => println(s"Deu bom com $a")
        case Failure(exception) => println(s"Deu ruim com $exception")
    }

    val pf: PartialFunction[Int,Int] = {
        case 1 => 34
        case _ => 55
    }

    //implicit conversions - implicit defs
    case class Person(name: String){
        def greet = println(s"Hi, my name is $name")
    }

    implicit def fromStringToPerson(name:String):Person = Person(name)

    "Tito".greet    //fromStringToPerson("Tito").greet

    //implicit conversion - implicit classes
    implicit class Dog(name: String) {
        def bark = println("Bark!")
    }

    "Toby".bark

}
