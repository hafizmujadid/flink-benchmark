package edu.tub.berlin.dima.bdapro.flink.benchmark

import java.util.Random

import org.apache.flink.streaming.api.functions.source.{ParallelSourceFunction, SourceFunction}

class WordCountSource extends ParallelSourceFunction[String]{
  var running = true
  override def run(sourceContext: SourceFunction.SourceContext[String]): Unit = {
    val data =
      """
        |Lorem ipsum dolor sit amet, consectetur adipiscing elit. Vivamus condimentum sagittis lacus,
        |laoreet luctus ligula laoreet ut. Vestibulum ullamcorper accumsan velit vel vehicula.
        |Proin tempor lacus arcu. Nunc at elit condimentum, semper nisi et, condimentum mi.
        |In venenatis blandit nibh at sollicitudin. Vestibulum dapibus mauris at orci maximus pellentesque.
        |Nullam id elementum ipsum. Suspendisse cursus lobortis viverra. Proin et erat at
        |mauris tincidunt porttitor vitae ac dui.
        |Donec vulputate lorem tortor, nec fermentum nibh bibendum vel. Lorem ipsum dolor sit amet,
        |consectetur adipiscing elit. Praesent dictum luctus massa, non euismod lacus.
        |Pellentesque condimentum dolor est, ut dapibus lectus luctus ac. Ut sagittis commodo arcu.
        |Integer nisi nulla, facilisis sit amet nulla quis, eleifend suscipit purus. Class aptent taciti
        |sociosqu ad litora torquent per conubia nostra, per inceptos himenaeos.
        |Aliquam euismod ultrices lorem, sit amet imperdiet est tincidunt vel.
        |Phasellus dictum justo sit amet ligula varius aliquet auctor et metus.
        |Fusce vitae tortor et nisi pulvinar vestibulum eget in risus. Donec ante ex,
        |placerat a lorem eget, ultricies bibendum purus. Nam sit amet neque non ante laoreet rutrum.
        |Nullam aliquet commodo urna, sed ullamcorper odio feugiat id.
        |Mauris nisi sapien, porttitor in condimentum nec, venenatis eu urna.
        |Pellentesque feugiat diam est, at rhoncus orci porttitor non.
        |Nulla luctus sem sit amet nisi consequat, id ornare ipsum dignissim.
        |Sed elementum elit nibh, eu condimentum orci viverra quis.
        |Aenean suscipit vitae felis non suscipit. Suspendisse pharetra turpis non eros semper dictum.
        |Etiam tincidunt venenatis venenatis. Praesent eget gravida lorem, ut congue diam.
        |Etiam facilisis elit at porttitor egestas. Praesent consequat, velit non vulputate convallis,
        |ligula diam sagittis urna, in venenatis nisi justo ut mauris. Vestibulum posuere sollicitudin mi,
        |et vulputate nisl fringilla non. Nulla ornare pretium velit a euismod.
        |Nunc sagittis venenatis vestibulum. Nunc sodales libero a est ornare ultricies.
        |Sed sed leo sed orci pellentesque ultrices. Mauris sollicitudin, sem quis placerat ornare,
        |velit arcu convallis ligula, pretium finibus nisl sapien vel sem.
        |Vivamus sit amet tortor id lorem consequat hendrerit. Nullam at dui risus.
      """.stripMargin

    while (true){
      sourceContext.collect(data)
      Thread.sleep(1000)
    }
  }

  override def cancel(): Unit = {
    running = false
  }
}
