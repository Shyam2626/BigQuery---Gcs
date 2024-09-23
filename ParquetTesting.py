from google.cloud import bigquery
import datetime

client = bigquery.Client()

table_id = 'superops-poc.shyamDataset.parquet-testing'

text = """ 

   
Lorem ipsum dolor sit amet, consectetur adipiscing elit. Cras fermentum facilisis diam, nec scelerisque arcu aliquet a. Suspendisse malesuada condimentum commodo. Maecenas vitae est ac quam efficitur ultrices a in augue. Fusce congue, dui scelerisque commodo sodales, odio metus rutrum erat, at varius nulla purus elementum nisl. Nulla condimentum nisl nibh, eget tempor urna pharetra ut. Nunc mi metus, rutrum ac finibus non, viverra nec erat. Morbi nec ipsum fermentum, commodo elit et, laoreet orci. Vestibulum imperdiet eros neque, a molestie massa sagittis vel. Aenean tempor tristique risus, quis vulputate lacus aliquam et. Sed et risus nulla. Vestibulum accumsan, ex eget interdum dictum, nibh arcu dignissim libero, sed ornare nisi ligula at tortor.

Suspendisse potenti. Proin non velit eros. Etiam ut dui congue, rhoncus sapien ac, euismod nisi. Curabitur sit amet semper velit, tincidunt egestas nisi. Suspendisse potenti. Nullam ultricies mauris eget tortor iaculis aliquam. Morbi in lectus magna. Vivamus sed iaculis arcu, vel porta turpis. Aliquam iaculis sodales sapien sit amet suscipit. Duis nec dignissim justo. In egestas dui lacus, sed elementum est tempus sed. Sed condimentum nisl a ex fermentum, nec tincidunt nisl sodales. Suspendisse potenti. Suspendisse consequat sollicitudin porttitor. Nullam at risus vitae nibh imperdiet lobortis.

Sed eu elit ante. Aliquam volutpat mi nec viverra viverra. Ut eleifend facilisis ante, nec porta nunc tristique et. Suspendisse potenti. In gravida lorem eu magna sodales tempus. Sed non nisi non felis feugiat ultricies in eget nibh. Nunc purus neque, pretium ut hendrerit eget, rutrum egestas nibh. Sed at mauris vitae dolor porta commodo id sed metus. Etiam malesuada velit aliquam enim luctus laoreet. Pellentesque cursus leo tempus tortor maximus, at mattis tortor interdum. Pellentesque auctor leo eu nunc laoreet lobortis. Sed congue est arcu, a tempor metus scelerisque ac.

Aliquam eget imperdiet neque. In ac lacinia nisl. Ut est sem, tristique sit amet eleifend sit amet, facilisis eget nunc. In sed ipsum lectus. Integer sagittis lacus in elit consectetur tincidunt. Mauris vitae tincidunt neque. Donec commodo et ligula sed posuere. Pellentesque ac turpis pulvinar, cursus risus in, venenatis sapien. Sed quis dolor est. Nunc dolor arcu, facilisis eget ultrices sit amet, accumsan vitae est. In gravida facilisis vestibulum. Proin felis odio, rutrum quis turpis in, pharetra rutrum leo. Praesent tempor feugiat neque, eget fermentum ante maximus sed. In ac congue felis. Nullam elementum interdum dignissim.

In placerat ullamcorper tellus ut vehicula. Praesent porttitor leo at sodales pharetra. Maecenas vestibulum vehicula turpis, eu interdum justo laoreet in. Vivamus placerat justo eu justo fringilla, id malesuada nisi porta. Nullam gravida sem quam, eu volutpat tellus accumsan non. Sed vitae tellus nisi. Cras ipsum velit, feugiat ac nibh in, interdum elementum nibh. Cras lacinia erat vel aliquam ultrices. Suspendisse sed urna dignissim, porta tortor eu, suscipit ex. Sed tempus metus vitae sem gravida aliquet. Phasellus non magna vitae magna euismod dignissim non scelerisque eros. Proin tincidunt lorem risus, eget pretium nulla euismod et. Proin semper a sem et congue. Pellentesque a suscipit tellus. Vivamus nisi diam, bibendum in vehicula nec, congue at diam. Etiam dictum lorem risus.

Morbi auctor bibendum est sit amet luctus. Aliquam mauris sapien, rutrum at ipsum vehicula, feugiat porta nisi. Integer ac tristique orci. Praesent bibendum ipsum id tellus bibendum, in congue turpis dapibus. Duis turpis nulla, hendrerit ac gravida et, lobortis sit amet ante. Curabitur non purus fringilla, porttitor tortor quis, feugiat magna. Aliquam eget est est.

Phasellus at eros velit. Ut pulvinar eros ut ex laoreet, non dapibus purus auctor. Duis lectus lorem, condimentum eu hendrerit vel, imperdiet vel justo. Donec nisl tortor, vulputate sit amet pharetra quis, consequat sed lacus. Quisque orci risus, bibendum non justo ac, ultrices rutrum lorem. Curabitur condimentum nisi magna, ut pharetra lectus iaculis non. Donec consectetur arcu quis sodales iaculis. In viverra eros condimentum magna scelerisque tempor. Donec gravida, tortor ut interdum feugiat, mauris eros tempor dui, blandit blandit leo nisi nec eros. Ut et tristique arcu. Nunc consectetur mattis quam, sed malesuada nulla molestie non. Sed rhoncus feugiat nisl, at malesuada enim rhoncus eu. In blandit velit ut dolor convallis condimentum. Cras luctus dictum mi, at vestibulum augue scelerisque tempus. Sed commodo est eu lacus commodo hendrerit. Mauris a massa lacus.

Quisque sodales luctus nisl, nec ornare lectus auctor vel. Nulla facilisi. Maecenas placerat condimentum auctor. Nunc ac sem rutrum, feugiat sapien in, cursus odio. Aliquam vitae risus elit. Morbi egestas tempor feugiat. Aliquam erat volutpat. Nam mattis maximus turpis nec accumsan. Cras vitae elit rhoncus, molestie lorem a, porttitor massa. Curabitur condimentum lacinia nunc id vehicula. Etiam at ex et sapien facilisis eleifend. Mauris feugiat lectus in scelerisque aliquet. Praesent et sapien et purus interdum elementum et cursus leo. Nam scelerisque ut lectus nec efficitur. Duis egestas metus et justo euismod lacinia. Vivamus vehicula, lectus a pulvinar luctus, erat quam placerat risus, placerat tristique diam tellus sit amet risus.

Donec eleifend, risus eget dignissim congue, ex nisl imperdiet dolor, eu ultrices odio nisi in nisl. In eget justo non odio commodo molestie in sed metus. Maecenas ut convallis sapien. Pellentesque ullamcorper efficitur erat vitae dapibus. Vivamus semper, velit at maximus euismod, ligula risus suscipit libero, a lacinia justo risus eu eros. Maecenas in lobortis felis, et mollis ligula. Aenean nec mi ultricies, viverra purus sit amet, accumsan tortor.

Nulla sit amet congue ante. In hac habitasse platea dictumst. Etiam nec tortor id neque sollicitudin suscipit. Proin nisi dolor, finibus ac ipsum et, suscipit dignissim est. Cras efficitur justo id tincidunt convallis. Nulla tincidunt ut enim ac placerat. Nullam condimentum in lacus vitae pretium. Curabitur porttitor dignissim lectus. Aenean fringilla neque ut iaculis blandit. Vestibulum at velit ut metus aliquet tincidunt. Mauris non felis ultricies, eleifend lorem nec, tristique tellus. Donec nisi diam, porta ac leo eget, auctor suscipit tortor.

Nullam tortor mi, posuere tempor eros ac, sagittis efficitur diam. Maecenas a purus felis. Integer et dui nisl. In aliquam purus vel scelerisque fermentum. Vivamus et feugiat risus. Maecenas sit amet ligula urna. Nulla sit amet bibendum odio. Sed eget quam tincidunt, tincidunt felis vitae, blandit nisl. Vivamus commodo.

"""

rows_to_insert = [
    {"text": text, "time": datetime.datetime.now().isoformat()}
]

errors = client.insert_rows_json(table_id, rows_to_insert)

if errors == []:
    print("New row has been added successfully.")
else:
    print(f"Encountered errors: {errors}")
