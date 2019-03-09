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
        |Vivamus sit amet tortor id lorem consequat hendrerit. Nullam at dui risus.Vestibulum ante ipsum primis in faucibus orci luctus et ultrices posuere cubilia Curae; Lorem ipsum dolor sit amet, consectetur adipiscing elit. Sed feugiat semper velit consequat facilisis. Etiam facilisis justo non iaculis dictum. Fusce turpis neque, pharetra ut odio eu, hendrerit rhoncus lacus. Nunc orci felis, imperdiet vel interdum quis, porta eu ipsum. Pellentesque dictum sem lacinia, auctor dui in, malesuada nunc. Maecenas sit amet mollis eros. Proin fringilla viverra ligula, sollicitudin viverra ante sollicitudin congue. Donec mollis felis eu libero malesuada, et lacinia risus interdum.
        |
        |Etiam vitae accumsan augue. Ut urna orci, malesuada ut nisi a, condimentum gravida magna. Nulla bibendum ex in vulputate sagittis. Nulla facilisi. Nullam faucibus et metus ac consequat. Quisque tempor eros velit, id mattis nibh aliquet a. Aenean tempor elit ut finibus auctor. Sed at imperdiet mauris. Vestibulum pharetra non lacus sed pulvinar. Sed pellentesque magna a eros volutpat ullamcorper. In hac habitasse platea dictumst. Donec ipsum mi, feugiat in eros sed, varius lacinia turpis. Donec vulputate tincidunt dui ac laoreet. Sed in eros dui. Pellentesque placerat tristique ligula eu finibus. Proin nec faucibus felis, eu commodo ipsum.
        |
        |Integer eu hendrerit diam, sed consectetur nunc. Aliquam a sem vitae leo fermentum faucibus quis at sem. Etiam blandit, quam quis fermentum varius, ante urna ultricies lectus, vel pellentesque ligula arcu nec elit. Donec placerat ante in enim scelerisque pretium. Donec et rhoncus erat. Aenean tempor nisi vitae augue tincidunt luctus. Nam condimentum dictum ante, et laoreet neque pellentesque id. Curabitur consectetur cursus neque aliquam porta. Ut interdum nunc nec nibh vestibulum, in sagittis metus facilisis. Pellentesque feugiat condimentum metus. Etiam venenatis quam at ante rhoncus vestibulum. Maecenas suscipit congue pellentesque. Vestibulum suscipit scelerisque fermentum. Nulla iaculis risus ac vulputate porttitor.
        |
        |Mauris nec metus vel dolor blandit faucibus et vel magna. Ut tincidunt ipsum non nunc dapibus, sed blandit mi condimentum. Quisque pharetra interdum quam nec feugiat. Sed pellentesque nulla et turpis blandit interdum. Curabitur at metus vitae augue elementum viverra. Sed mattis lorem non enim fermentum finibus. Sed at dui in magna dignissim accumsan. Proin tincidunt ultricies cursus. Maecenas tincidunt magna at urna faucibus lacinia.
        |
        |Quisque venenatis justo sit amet tortor condimentum, nec tincidunt tellus viverra. Morbi risus ipsum, consequat convallis malesuada non, fermentum non velit. Nulla facilisis orci eget ligula mattis fermentum. Aliquam vel velit ultricies, sollicitudin nibh eu, congue velit. Donec nulla lorem, euismod id cursus at, sollicitudin et arcu. Proin vitae tincidunt ipsum. Vivamus elementum eleifend justo, placerat interdum nulla rutrum id.
        |
        |Phasellus fringilla luctus magna, a finibus justo dapibus a. Nam risus felis, rhoncus eget diam sit amet, congue facilisis nibh. Interdum et malesuada fames ac ante ipsum primis in faucibus. Praesent consequat euismod diam, eget volutpat magna convallis at. Mauris placerat pellentesque imperdiet. Nulla porta scelerisque enim, et scelerisque neque bibendum in. Proin eget turpis nisi. Suspendisse ut est a erat egestas eleifend at euismod arcu. Donec aliquet, nisi sed faucibus condimentum, nisi metus dictum eros, nec dignissim justo odio id nulla. Pellentesque habitant morbi tristique senectus et netus et malesuada fames ac turpis egestas. Maecenas sollicitudin, justo id elementum eleifend, justo neque aliquet nibh, finibus malesuada metus erat eget neque. Suspendisse nec auctor orci. Aenean et vestibulum nulla.
        |Nullam hendrerit augue tristique, commodo metus id, sodales lorem. Etiam feugiat dui est, vitae auctor risus convallis non.
        |
        |Maecenas turpis enim, consectetur eget lectus eu, hendrerit posuere lacus. Praesent efficitur, felis eget dapibus consectetur, nisi massa dignissim enim, nec semper dolor est eu urna. Nullam ut sodales lorem. Aliquam dapibus faucibus diam. Vestibulum vel magna et dolor gravida imperdiet ut sit amet sem. Lorem ipsum dolor sit amet, consectetur adipiscing elit. Curabitur elementum metus tincidunt nulla euismod ultricies. Duis elementum nec neque in porttitor. Nulla sagittis lorem elit, et consectetur ante laoreet eu. Maecenas nulla tellus, scelerisque ac erat sed, fermentum dapibus metus. Donec tincidunt fermentum molestie.
        |
        |Sed consequat mi at maximus faucibus. Pellentesque aliquet tincidunt sapien vel auctor. Pellentesque habitant morbi tristique senectus et netus et malesuada fames ac turpis egestas. Praesent accumsan nunc eget leo aliquam, facilisis hendrerit turpis egestas. Morbi in ultricies mauris, a eleifend turpis. Quisque fringilla massa iaculis risus ultrices, sit amet tincidunt dui varius. Quisque maximus porta tristique. Proin tincidunt, turpis ut tempor pretium, lectus ipsum ullamcorper leo, ac tincidunt felis dui non leo. Aenean porta augue ligula, non consequat ipsum aliquet et. Suspendisse ut suscipit ex. Pellentesque vitae lacinia arcu. Curabitur eget tincidunt nulla, non bibendum metus. Nullam mi ipsum, eleifend vitae tortor pulvinar, facilisis sollicitudin ipsum.
        |
        |Vestibulum molestie risus lorem, at feugiat lorem congue sed. Phasellus ullamcorper laoreet enim, nec aliquam turpis scelerisque et. Etiam dictum metus in elit aliquam dapibus. Vivamus vel lectus velit. Nam sed purus luctus, commodo dui quis, malesuada dui. Nulla porttitor aliquet elit sit amet viverra. Proin tempor nulla urna, non aliquet metus maximus quis. Aliquam ac lectus nec mi aliquam sagittis. Quisque venenatis quam eget nisl tempor, egestas rutrum eros eleifend. Nullam venenatis commodo velit, non tempor mauris fermentum ut. In a metus quis erat cursus sagittis. Donec congue nisl in viverra egestas.
        |
        |Vestibulum facilisis ligula magna, eu ornare lectus varius et. Mauris facilisis faucibus quam, quis mollis eros convallis non. Interdum et malesuada fames ac ante ipsum primis in faucibus. Praesent sit amet rutrum erat. Suspendisse potenti. Donec lorem mi, sagittis a fringilla sit amet, sagittis bibendum mauris. In in diam et lorem rutrum eleifend a et felis. Sed ac magna quis enim faucibus dictum. Suspendisse blandit enim eu ex laoreet gravida.
        |
        |Suspendisse sed semper felis. Etiam mattis magna mi, suscipit ullamcorper tellus euismod sed. Aenean congue scelerisque ligula id sodales. Class aptent taciti sociosqu ad litora torquent per conubia nostra, per inceptos himenaeos. Nunc sem lectus, gravida ac dui non, pharetra posuere leo. Maecenas lacus libero, facilisis et elit vitae, commodo facilisis sem. Vivamus id nisl nulla. Integer at maximus dui. Ut a tincidunt lorem. Vivamus vitae ligula vel lacus cursus condimentum. Phasellus quis mauris lobortis, finibus lorem in, vulputate ex. Lorem ipsum dolor sit amet, consectetur adipiscing elit. Sed faucibus aliquam metus, quis varius elit porttitor id. Vivamus dignissim sollicitudin scelerisque. Morbi tincidunt, dolor quis vehicula consequat, dui diam condimentum nunc, vitae scelerisque odio libero nec ligula. Vestibulum ante ipsum primis in faucibus orci luctus et ultrices posuere cubilia Curae;
      """.stripMargin

    while (true){
      sourceContext.collect(data)
      Thread.sleep(30)
    }
  }

  override def cancel(): Unit = {
    running = false
  }
}
