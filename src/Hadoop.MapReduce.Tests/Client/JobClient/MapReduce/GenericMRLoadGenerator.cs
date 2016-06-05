using System;
using System.Collections.Generic;
using System.IO;
using System.Text;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.IO;
using Org.Apache.Hadoop.Mapred;
using Org.Apache.Hadoop.Mapreduce.Lib.Input;
using Org.Apache.Hadoop.Mapreduce.Lib.Output;
using Org.Apache.Hadoop.Util;
using Sharpen;

namespace Org.Apache.Hadoop.Mapreduce
{
	public class GenericMRLoadGenerator : Configured, Tool
	{
		public static string MapPreservePercent = "mapreduce.loadgen.sort.map.preserve.percent";

		public static string ReducePreservePercent = "mapreduce.loadgen.sort.reduce.preserve.percent";

		public static string IndirectInputFormat = "mapreduce.loadgen.indirect.input.format";

		public static string IndirectInputFile = "mapreduce.loadgen.indirect.input.file";

		protected internal static int PrintUsage()
		{
			System.Console.Error.WriteLine("Usage: [-m <maps>] [-r <reduces>]\n" + "       [-keepmap <percent>] [-keepred <percent>]\n"
				 + "       [-indir <path>] [-outdir <path]\n" + "       [-inFormat[Indirect] <InputFormat>] [-outFormat <OutputFormat>]\n"
				 + "       [-outKey <WritableComparable>] [-outValue <Writable>]\n");
			GenericOptionsParser.PrintGenericCommandUsage(System.Console.Error);
			return -1;
		}

		/// <summary>Configure a job given argv.</summary>
		/// <exception cref="System.IO.IOException"/>
		public static bool ParseArgs(string[] argv, Job job)
		{
			if (argv.Length < 1)
			{
				return 0 == PrintUsage();
			}
			for (int i = 0; i < argv.Length; ++i)
			{
				if (argv.Length == i + 1)
				{
					System.Console.Out.WriteLine("ERROR: Required parameter missing from " + argv[i]);
					return 0 == PrintUsage();
				}
				try
				{
					if ("-r".Equals(argv[i]))
					{
						job.SetNumReduceTasks(System.Convert.ToInt32(argv[++i]));
					}
					else
					{
						if ("-inFormat".Equals(argv[i]))
						{
							job.SetInputFormatClass(Sharpen.Runtime.GetType(argv[++i]).AsSubclass<InputFormat
								>());
						}
						else
						{
							if ("-outFormat".Equals(argv[i]))
							{
								job.SetOutputFormatClass(Sharpen.Runtime.GetType(argv[++i]).AsSubclass<OutputFormat
									>());
							}
							else
							{
								if ("-outKey".Equals(argv[i]))
								{
									job.SetOutputKeyClass(Sharpen.Runtime.GetType(argv[++i]).AsSubclass<WritableComparable
										>());
								}
								else
								{
									if ("-outValue".Equals(argv[i]))
									{
										job.SetOutputValueClass(Sharpen.Runtime.GetType(argv[++i]).AsSubclass<Writable>()
											);
									}
									else
									{
										if ("-keepmap".Equals(argv[i]))
										{
											job.GetConfiguration().Set(MapPreservePercent, argv[++i]);
										}
										else
										{
											if ("-keepred".Equals(argv[i]))
											{
												job.GetConfiguration().Set(ReducePreservePercent, argv[++i]);
											}
											else
											{
												if ("-outdir".Equals(argv[i]))
												{
													FileOutputFormat.SetOutputPath(job, new Path(argv[++i]));
												}
												else
												{
													if ("-indir".Equals(argv[i]))
													{
														FileInputFormat.AddInputPaths(job, argv[++i]);
													}
													else
													{
														if ("-inFormatIndirect".Equals(argv[i]))
														{
															job.GetConfiguration().SetClass(IndirectInputFormat, Sharpen.Runtime.GetType(argv
																[++i]).AsSubclass<InputFormat>(), typeof(InputFormat));
															job.SetInputFormatClass(typeof(GenericMRLoadGenerator.IndirectInputFormat));
														}
														else
														{
															System.Console.Out.WriteLine("Unexpected argument: " + argv[i]);
															return 0 == PrintUsage();
														}
													}
												}
											}
										}
									}
								}
							}
						}
					}
				}
				catch (FormatException)
				{
					System.Console.Out.WriteLine("ERROR: Integer expected instead of " + argv[i]);
					return 0 == PrintUsage();
				}
				catch (Exception e)
				{
					throw (IOException)Sharpen.Extensions.InitCause(new IOException(), e);
				}
			}
			return true;
		}

		/// <exception cref="System.Exception"/>
		public virtual int Run(string[] argv)
		{
			Job job = Job.GetInstance(GetConf());
			job.SetJarByClass(typeof(GenericMRLoadGenerator));
			job.SetMapperClass(typeof(GenericMRLoadGenerator.SampleMapper));
			job.SetReducerClass(typeof(GenericMRLoadGenerator.SampleReducer));
			if (!ParseArgs(argv, job))
			{
				return -1;
			}
			Configuration conf = job.GetConfiguration();
			if (null == FileOutputFormat.GetOutputPath(job))
			{
				// No output dir? No writes
				job.SetOutputFormatClass(typeof(NullOutputFormat));
			}
			if (0 == FileInputFormat.GetInputPaths(job).Length)
			{
				// No input dir? Generate random data
				System.Console.Error.WriteLine("No input path; ignoring InputFormat");
				ConfRandom(job);
			}
			else
			{
				if (null != conf.GetClass(IndirectInputFormat, null))
				{
					// specified IndirectInputFormat? Build src list
					JobClient jClient = new JobClient(conf);
					Path tmpDir = new Path("/tmp");
					Random r = new Random();
					Path indirInputFile = new Path(tmpDir, Sharpen.Extensions.ToString(r.Next(int.MaxValue
						), 36) + "_files");
					conf.Set(IndirectInputFile, indirInputFile.ToString());
					SequenceFile.Writer writer = SequenceFile.CreateWriter(tmpDir.GetFileSystem(conf)
						, conf, indirInputFile, typeof(LongWritable), typeof(Text), SequenceFile.CompressionType
						.None);
					try
					{
						foreach (Path p in FileInputFormat.GetInputPaths(job))
						{
							FileSystem fs = p.GetFileSystem(conf);
							Stack<Path> pathstack = new Stack<Path>();
							pathstack.Push(p);
							while (!pathstack.Empty())
							{
								foreach (FileStatus stat in fs.ListStatus(pathstack.Pop()))
								{
									if (stat.IsDirectory())
									{
										if (!stat.GetPath().GetName().StartsWith("_"))
										{
											pathstack.Push(stat.GetPath());
										}
									}
									else
									{
										writer.Sync();
										writer.Append(new LongWritable(stat.GetLen()), new Text(stat.GetPath().ToUri().ToString
											()));
									}
								}
							}
						}
					}
					finally
					{
						writer.Close();
					}
				}
			}
			DateTime startTime = new DateTime();
			System.Console.Out.WriteLine("Job started: " + startTime);
			int ret = job.WaitForCompletion(true) ? 0 : 1;
			DateTime endTime = new DateTime();
			System.Console.Out.WriteLine("Job ended: " + endTime);
			System.Console.Out.WriteLine("The job took " + (endTime.GetTime() - startTime.GetTime
				()) / 1000 + " seconds.");
			return ret;
		}

		/// <summary>Main driver/hook into ToolRunner.</summary>
		/// <exception cref="System.Exception"/>
		public static void Main(string[] argv)
		{
			int res = ToolRunner.Run(new Configuration(), new GenericMRLoadGenerator(), argv);
			System.Environment.Exit(res);
		}

		internal class RandomInputFormat : InputFormat<Text, Text>
		{
			public override IList<InputSplit> GetSplits(JobContext job)
			{
				int numSplits = job.GetConfiguration().GetInt(MRJobConfig.NumMaps, 1);
				IList<InputSplit> splits = new AList<InputSplit>();
				for (int i = 0; i < numSplits; ++i)
				{
					splits.AddItem(new GenericMRLoadGenerator.IndirectInputFormat.IndirectSplit(new Path
						("ignore" + i), 1));
				}
				return splits;
			}

			/// <exception cref="System.IO.IOException"/>
			public override RecordReader<Text, Text> CreateRecordReader(InputSplit split, TaskAttemptContext
				 context)
			{
				GenericMRLoadGenerator.IndirectInputFormat.IndirectSplit clSplit = (GenericMRLoadGenerator.IndirectInputFormat.IndirectSplit
					)split;
				return new _RecordReader_222(clSplit);
			}

			private sealed class _RecordReader_222 : RecordReader<Text, Text>
			{
				public _RecordReader_222(GenericMRLoadGenerator.IndirectInputFormat.IndirectSplit
					 clSplit)
				{
					this.clSplit = clSplit;
					this.once = true;
					this.key = new Text();
					this.value = new Text();
				}

				internal bool once;

				internal Text key;

				internal Text value;

				public override bool NextKeyValue()
				{
					if (this.once)
					{
						this.key.Set(clSplit.GetPath().ToString());
						this.once = false;
						return true;
					}
					return false;
				}

				/// <exception cref="System.IO.IOException"/>
				/// <exception cref="System.Exception"/>
				public override void Initialize(InputSplit split, TaskAttemptContext context)
				{
				}

				public override Text GetCurrentKey()
				{
					return this.key;
				}

				public override Text GetCurrentValue()
				{
					return this.value;
				}

				public override void Close()
				{
				}

				public override float GetProgress()
				{
					return 0.0f;
				}

				private readonly GenericMRLoadGenerator.IndirectInputFormat.IndirectSplit clSplit;
			}
		}

		internal enum Counters
		{
			RecordsWritten,
			BytesWritten
		}

		internal class RandomMapOutput : Mapper<Text, Text, Text, Text>
		{
			internal StringBuilder sentence = new StringBuilder();

			internal int keymin;

			internal int keymax;

			internal int valmin;

			internal int valmax;

			internal long bytesToWrite;

			internal Random r = new Random();

			private int GenerateSentence(Org.Apache.Hadoop.IO.Text t, int noWords)
			{
				sentence.Length = 0;
				--noWords;
				for (int i = 0; i < noWords; ++i)
				{
					sentence.Append(words[r.Next(words.Length)]);
					sentence.Append(" ");
				}
				if (noWords >= 0)
				{
					sentence.Append(words[r.Next(words.Length)]);
				}
				t.Set(sentence.ToString());
				return sentence.Length;
			}

			protected override void Setup(Mapper.Context context)
			{
				Configuration conf = new Configuration();
				bytesToWrite = conf.GetLong(RandomTextWriter.BytesPerMap, 1 * 1024 * 1024 * 1024);
				keymin = conf.GetInt(RandomTextWriter.MinKey, 5);
				keymax = conf.GetInt(RandomTextWriter.MaxKey, 10);
				valmin = conf.GetInt(RandomTextWriter.MinValue, 5);
				valmax = conf.GetInt(RandomTextWriter.MaxValue, 10);
			}

			/// <exception cref="System.IO.IOException"/>
			/// <exception cref="System.Exception"/>
			protected override void Map(Org.Apache.Hadoop.IO.Text key, Org.Apache.Hadoop.IO.Text
				 val, Mapper.Context context)
			{
				long acc = 0L;
				long recs = 0;
				int keydiff = keymax - keymin;
				int valdiff = valmax - valmin;
				for (long i = 0L; acc < bytesToWrite; ++i)
				{
					int recacc = 0;
					recacc += GenerateSentence(key, keymin + (0 == keydiff ? 0 : r.Next(keydiff)));
					recacc += GenerateSentence(val, valmin + (0 == valdiff ? 0 : r.Next(valdiff)));
					context.Write(key, val);
					++recs;
					acc += recacc;
					context.GetCounter(GenericMRLoadGenerator.Counters.BytesWritten).Increment(recacc
						);
					context.GetCounter(GenericMRLoadGenerator.Counters.RecordsWritten).Increment(1);
					context.SetStatus(acc + "/" + (bytesToWrite - acc) + " bytes");
				}
				context.SetStatus("Wrote " + recs + " records");
			}
		}

		/// <summary>When no input dir is specified, generate random data.</summary>
		/// <exception cref="System.IO.IOException"/>
		protected internal static void ConfRandom(Job job)
		{
			// from RandomWriter
			job.SetInputFormatClass(typeof(GenericMRLoadGenerator.RandomInputFormat));
			job.SetMapperClass(typeof(GenericMRLoadGenerator.RandomMapOutput));
			Configuration conf = job.GetConfiguration();
			ClusterStatus cluster = new JobClient(conf).GetClusterStatus();
			int numMapsPerHost = conf.GetInt(RandomTextWriter.MapsPerHost, 10);
			long numBytesToWritePerMap = conf.GetLong(RandomTextWriter.BytesPerMap, 1 * 1024 
				* 1024 * 1024);
			if (numBytesToWritePerMap == 0)
			{
				throw new IOException("Cannot have " + RandomTextWriter.BytesPerMap + " set to 0"
					);
			}
			long totalBytesToWrite = conf.GetLong(RandomTextWriter.TotalBytes, numMapsPerHost
				 * numBytesToWritePerMap * cluster.GetTaskTrackers());
			int numMaps = (int)(totalBytesToWrite / numBytesToWritePerMap);
			if (numMaps == 0 && totalBytesToWrite > 0)
			{
				numMaps = 1;
				conf.SetLong(RandomTextWriter.BytesPerMap, totalBytesToWrite);
			}
			conf.SetInt(MRJobConfig.NumMaps, numMaps);
		}

		internal abstract class SampleMapBase<K, V> : Mapper<K, V, K, V>
			where K : WritableComparable<object>
			where V : Writable
		{
			private long total;

			private long kept = 0;

			private float keep;

			// Sampling //
			protected override void Setup(Mapper.Context context)
			{
				this.keep = context.GetConfiguration().GetFloat(MapPreservePercent, (float)100.0)
					 / (float)100.0;
			}

			/// <exception cref="System.IO.IOException"/>
			/// <exception cref="System.Exception"/>
			protected internal virtual void Emit(K key, V val, Mapper.Context context)
			{
				++total;
				while ((float)kept / total < keep)
				{
					++kept;
					context.Write(key, val);
				}
			}
		}

		internal abstract class SampleReduceBase<K, V> : Reducer<K, V, K, V>
			where K : WritableComparable<object>
			where V : Writable
		{
			private long total;

			private long kept = 0;

			private float keep;

			protected override void Setup(Reducer.Context context)
			{
				this.keep = context.GetConfiguration().GetFloat(ReducePreservePercent, (float)100.0
					) / (float)100.0;
			}

			/// <exception cref="System.IO.IOException"/>
			/// <exception cref="System.Exception"/>
			protected internal virtual void Emit(K key, V val, Reducer.Context context)
			{
				++total;
				while ((float)kept / total < keep)
				{
					++kept;
					context.Write(key, val);
				}
			}
		}

		public class SampleMapper<K, V> : GenericMRLoadGenerator.SampleMapBase<K, V>
			where K : WritableComparable<object>
			where V : Writable
		{
			/// <exception cref="System.IO.IOException"/>
			/// <exception cref="System.Exception"/>
			protected override void Map(K key, V val, Mapper.Context context)
			{
				Emit(key, val, context);
			}
		}

		public class SampleReducer<K, V> : GenericMRLoadGenerator.SampleReduceBase<K, V>
			where K : WritableComparable<object>
			where V : Writable
		{
			/// <exception cref="System.IO.IOException"/>
			/// <exception cref="System.Exception"/>
			protected override void Reduce(K key, IEnumerable<V> values, Reducer.Context context
				)
			{
				foreach (V value in values)
				{
					Emit(key, value, context);
				}
			}
		}

		/// <summary>
		/// Obscures the InputFormat and location information to simulate maps
		/// reading input from arbitrary locations (&quot;indirect&quot; reads).
		/// </summary>
		internal class IndirectInputFormat<K, V> : InputFormat<K, V>
		{
			internal class IndirectSplit : InputSplit
			{
				internal Path file;

				internal long len;

				public IndirectSplit()
				{
				}

				public IndirectSplit(Path file, long len)
				{
					// Indirect reads //
					this.file = file;
					this.len = len;
				}

				public virtual Path GetPath()
				{
					return file;
				}

				public override long GetLength()
				{
					return len;
				}

				/// <exception cref="System.IO.IOException"/>
				public override string[] GetLocations()
				{
					return new string[] {  };
				}

				/// <exception cref="System.IO.IOException"/>
				public virtual void Write(DataOutput @out)
				{
					WritableUtils.WriteString(@out, file.ToString());
					WritableUtils.WriteVLong(@out, len);
				}

				/// <exception cref="System.IO.IOException"/>
				public virtual void ReadFields(DataInput @in)
				{
					file = new Path(WritableUtils.ReadString(@in));
					len = WritableUtils.ReadVLong(@in);
				}
			}

			/// <exception cref="System.IO.IOException"/>
			public override IList<InputSplit> GetSplits(JobContext job)
			{
				Configuration conf = job.GetConfiguration();
				Path src = new Path(conf.Get(IndirectInputFile, null));
				FileSystem fs = src.GetFileSystem(conf);
				IList<InputSplit> splits = new AList<InputSplit>();
				LongWritable key = new LongWritable();
				Org.Apache.Hadoop.IO.Text value = new Org.Apache.Hadoop.IO.Text();
				for (SequenceFile.Reader sl = new SequenceFile.Reader(fs, src, conf); sl.Next(key
					, value); )
				{
					splits.AddItem(new GenericMRLoadGenerator.IndirectInputFormat.IndirectSplit(new Path
						(value.ToString()), key.Get()));
				}
				return splits;
			}

			/// <exception cref="System.IO.IOException"/>
			/// <exception cref="System.Exception"/>
			public override RecordReader<K, V> CreateRecordReader(InputSplit split, TaskAttemptContext
				 context)
			{
				Configuration conf = context.GetConfiguration();
				InputFormat<K, V> indirIF = (InputFormat)ReflectionUtils.NewInstance(conf.GetClass
					(IndirectInputFormat, typeof(SequenceFileInputFormat)), conf);
				GenericMRLoadGenerator.IndirectInputFormat.IndirectSplit @is = ((GenericMRLoadGenerator.IndirectInputFormat.IndirectSplit
					)split);
				return indirIF.CreateRecordReader(new FileSplit(@is.GetPath(), 0, @is.GetLength()
					, (string[])null), context);
			}
		}

		/// <summary>A random list of 1000 words from /usr/share/dict/words</summary>
		private static readonly string[] words = new string[] { "diurnalness", "Homoiousian"
			, "spiranthic", "tetragynian", "silverhead", "ungreat", "lithograph", "exploiter"
			, "physiologian", "by", "hellbender", "Filipendula", "undeterring", "antiscolic"
			, "pentagamist", "hypoid", "cacuminal", "sertularian", "schoolmasterism", "nonuple"
			, "gallybeggar", "phytonic", "swearingly", "nebular", "Confervales", "thermochemically"
			, "characinoid", "cocksuredom", "fallacious", "feasibleness", "debromination", "playfellowship"
			, "tramplike", "testa", "participatingly", "unaccessible", "bromate", "experientialist"
			, "roughcast", "docimastical", "choralcelo", "blightbird", "peptonate", "sombreroed"
			, "unschematized", "antiabolitionist", "besagne", "mastication", "bromic", "sviatonosite"
			, "cattimandoo", "metaphrastical", "endotheliomyoma", "hysterolysis", "unfulminated"
			, "Hester", "oblongly", "blurredness", "authorling", "chasmy", "Scorpaenidae", "toxihaemia"
			, "Dictograph", "Quakerishly", "deaf", "timbermonger", "strammel", "Thraupidae", 
			"seditious", "plerome", "Arneb", "eristically", "serpentinic", "glaumrie", "socioromantic"
			, "apocalypst", "tartrous", "Bassaris", "angiolymphoma", "horsefly", "kenno", "astronomize"
			, "euphemious", "arsenide", "untongued", "parabolicness", "uvanite", "helpless", 
			"gemmeous", "stormy", "templar", "erythrodextrin", "comism", "interfraternal", "preparative"
			, "parastas", "frontoorbital", "Ophiosaurus", "diopside", "serosanguineous", "ununiformly"
			, "karyological", "collegian", "allotropic", "depravity", "amylogenesis", "reformatory"
			, "epidymides", "pleurotropous", "trillium", "dastardliness", "coadvice", "embryotic"
			, "benthonic", "pomiferous", "figureheadship", "Megaluridae", "Harpa", "frenal", 
			"commotion", "abthainry", "cobeliever", "manilla", "spiciferous", "nativeness", 
			"obispo", "monilioid", "biopsic", "valvula", "enterostomy", "planosubulate", "pterostigma"
			, "lifter", "triradiated", "venialness", "tum", "archistome", "tautness", "unswanlike"
			, "antivenin", "Lentibulariaceae", "Triphora", "angiopathy", "anta", "Dawsonia", 
			"becomma", "Yannigan", "winterproof", "antalgol", "harr", "underogating", "ineunt"
			, "cornberry", "flippantness", "scyphostoma", "approbation", "Ghent", "Macraucheniidae"
			, "scabbiness", "unanatomized", "photoelasticity", "eurythermal", "enation", "prepavement"
			, "flushgate", "subsequentially", "Edo", "antihero", "Isokontae", "unforkedness"
			, "porriginous", "daytime", "nonexecutive", "trisilicic", "morphiomania", "paranephros"
			, "botchedly", "impugnation", "Dodecatheon", "obolus", "unburnt", "provedore", "Aktistetae"
			, "superindifference", "Alethea", "Joachimite", "cyanophilous", "chorograph", "brooky"
			, "figured", "periclitation", "quintette", "hondo", "ornithodelphous", "unefficient"
			, "pondside", "bogydom", "laurinoxylon", "Shiah", "unharmed", "cartful", "noncrystallized"
			, "abusiveness", "cromlech", "japanned", "rizzomed", "underskin", "adscendent", 
			"allectory", "gelatinousness", "volcano", "uncompromisingly", "cubit", "idiotize"
			, "unfurbelowed", "undinted", "magnetooptics", "Savitar", "diwata", "ramosopalmate"
			, "Pishquow", "tomorn", "apopenptic", "Haversian", "Hysterocarpus", "ten", "outhue"
			, "Bertat", "mechanist", "asparaginic", "velaric", "tonsure", "bubble", "Pyrales"
			, "regardful", "glyphography", "calabazilla", "shellworker", "stradametrical", "havoc"
			, "theologicopolitical", "sawdust", "diatomaceous", "jajman", "temporomastoid", 
			"Serrifera", "Ochnaceae", "aspersor", "trailmaking", "Bishareen", "digitule", "octogynous"
			, "epididymitis", "smokefarthings", "bacillite", "overcrown", "mangonism", "sirrah"
			, "undecorated", "psychofugal", "bismuthiferous", "rechar", "Lemuridae", "frameable"
			, "thiodiazole", "Scanic", "sportswomanship", "interruptedness", "admissory", "osteopaedion"
			, "tingly", "tomorrowness", "ethnocracy", "trabecular", "vitally", "fossilism", 
			"adz", "metopon", "prefatorial", "expiscate", "diathermacy", "chronist", "nigh", 
			"generalizable", "hysterogen", "aurothiosulphuric", "whitlowwort", "downthrust", 
			"Protestantize", "monander", "Itea", "chronographic", "silicize", "Dunlop", "eer"
			, "componental", "spot", "pamphlet", "antineuritic", "paradisean", "interruptor"
			, "debellator", "overcultured", "Florissant", "hyocholic", "pneumatotherapy", "tailoress"
			, "rave", "unpeople", "Sebastian", "thermanesthesia", "Coniferae", "swacking", "posterishness"
			, "ethmopalatal", "whittle", "analgize", "scabbardless", "naught", "symbiogenetically"
			, "trip", "parodist", "columniform", "trunnel", "yawler", "goodwill", "pseudohalogen"
			, "swangy", "cervisial", "mediateness", "genii", "imprescribable", "pony", "consumptional"
			, "carposporangial", "poleax", "bestill", "subfebrile", "sapphiric", "arrowworm"
			, "qualminess", "ultraobscure", "thorite", "Fouquieria", "Bermudian", "prescriber"
			, "elemicin", "warlike", "semiangle", "rotular", "misthread", "returnability", "seraphism"
			, "precostal", "quarried", "Babylonism", "sangaree", "seelful", "placatory", "pachydermous"
			, "bozal", "galbulus", "spermaphyte", "cumbrousness", "pope", "signifier", "Endomycetaceae"
			, "shallowish", "sequacity", "periarthritis", "bathysphere", "pentosuria", "Dadaism"
			, "spookdom", "Consolamentum", "afterpressure", "mutter", "louse", "ovoviviparous"
			, "corbel", "metastoma", "biventer", "Hydrangea", "hogmace", "seizing", "nonsuppressed"
			, "oratorize", "uncarefully", "benzothiofuran", "penult", "balanocele", "macropterous"
			, "dishpan", "marten", "absvolt", "jirble", "parmelioid", "airfreighter", "acocotl"
			, "archesporial", "hypoplastral", "preoral", "quailberry", "cinque", "terrestrially"
			, "stroking", "limpet", "moodishness", "canicule", "archididascalian", "pompiloid"
			, "overstaid", "introducer", "Italical", "Christianopaganism", "prescriptible", 
			"subofficer", "danseuse", "cloy", "saguran", "frictionlessly", "deindividualization"
			, "Bulanda", "ventricous", "subfoliar", "basto", "scapuloradial", "suspend", "stiffish"
			, "Sphenodontidae", "eternal", "verbid", "mammonish", "upcushion", "barkometer", 
			"concretion", "preagitate", "incomprehensible", "tristich", "visceral", "hemimelus"
			, "patroller", "stentorophonic", "pinulus", "kerykeion", "brutism", "monstership"
			, "merciful", "overinstruct", "defensibly", "bettermost", "splenauxe", "Mormyrus"
			, "unreprimanded", "taver", "ell", "proacquittal", "infestation", "overwoven", "Lincolnlike"
			, "chacona", "Tamil", "classificational", "lebensraum", "reeveland", "intuition"
			, "Whilkut", "focaloid", "Eleusinian", "micromembrane", "byroad", "nonrepetition"
			, "bacterioblast", "brag", "ribaldrous", "phytoma", "counteralliance", "pelvimetry"
			, "pelf", "relaster", "thermoresistant", "aneurism", "molossic", "euphonym", "upswell"
			, "ladhood", "phallaceous", "inertly", "gunshop", "stereotypography", "laryngic"
			, "refasten", "twinling", "oflete", "hepatorrhaphy", "electrotechnics", "cockal"
			, "guitarist", "topsail", "Cimmerianism", "larklike", "Llandovery", "pyrocatechol"
			, "immatchable", "chooser", "metrocratic", "craglike", "quadrennial", "nonpoisonous"
			, "undercolored", "knob", "ultratense", "balladmonger", "slait", "sialadenitis", 
			"bucketer", "magnificently", "unstipulated", "unscourged", "unsupercilious", "packsack"
			, "pansophism", "soorkee", "percent", "subirrigate", "champer", "metapolitics", 
			"spherulitic", "involatile", "metaphonical", "stachyuraceous", "speckedness", "bespin"
			, "proboscidiform", "gul", "squit", "yeelaman", "peristeropode", "opacousness", 
			"shibuichi", "retinize", "yote", "misexposition", "devilwise", "pumpkinification"
			, "vinny", "bonze", "glossing", "decardinalize", "transcortical", "serphoid", "deepmost"
			, "guanajuatite", "wemless", "arval", "lammy", "Effie", "Saponaria", "tetrahedral"
			, "prolificy", "excerpt", "dunkadoo", "Spencerism", "insatiately", "Gilaki", "oratorship"
			, "arduousness", "unbashfulness", "Pithecolobium", "unisexuality", "veterinarian"
			, "detractive", "liquidity", "acidophile", "proauction", "sural", "totaquina", "Vichyite"
			, "uninhabitedness", "allegedly", "Gothish", "manny", "Inger", "flutist", "ticktick"
			, "Ludgatian", "homotransplant", "orthopedical", "diminutively", "monogoneutic", 
			"Kenipsim", "sarcologist", "drome", "stronghearted", "Fameuse", "Swaziland", "alen"
			, "chilblain", "beatable", "agglomeratic", "constitutor", "tendomucoid", "porencephalous"
			, "arteriasis", "boser", "tantivy", "rede", "lineamental", "uncontradictableness"
			, "homeotypical", "masa", "folious", "dosseret", "neurodegenerative", "subtransverse"
			, "Chiasmodontidae", "palaeotheriodont", "unstressedly", "chalcites", "piquantness"
			, "lampyrine", "Aplacentalia", "projecting", "elastivity", "isopelletierin", "bladderwort"
			, "strander", "almud", "iniquitously", "theologal", "bugre", "chargeably", "imperceptivity"
			, "meriquinoidal", "mesophyte", "divinator", "perfunctory", "counterappellant", 
			"synovial", "charioteer", "crystallographical", "comprovincial", "infrastapedial"
			, "pleasurehood", "inventurous", "ultrasystematic", "subangulated", "supraoesophageal"
			, "Vaishnavism", "transude", "chrysochrous", "ungrave", "reconciliable", "uninterpleaded"
			, "erlking", "wherefrom", "aprosopia", "antiadiaphorist", "metoxazine", "incalculable"
			, "umbellic", "predebit", "foursquare", "unimmortal", "nonmanufacture", "slangy"
			, "predisputant", "familist", "preaffiliate", "friarhood", "corelysis", "zoonitic"
			, "halloo", "paunchy", "neuromimesis", "aconitine", "hackneyed", "unfeeble", "cubby"
			, "autoschediastical", "naprapath", "lyrebird", "inexistency", "leucophoenicite"
			, "ferrogoslarite", "reperuse", "uncombable", "tambo", "propodiale", "diplomatize"
			, "Russifier", "clanned", "corona", "michigan", "nonutilitarian", "transcorporeal"
			, "bought", "Cercosporella", "stapedius", "glandularly", "pictorially", "weism", 
			"disilane", "rainproof", "Caphtor", "scrubbed", "oinomancy", "pseudoxanthine", "nonlustrous"
			, "redesertion", "Oryzorictinae", "gala", "Mycogone", "reappreciate", "cyanoguanidine"
			, "seeingness", "breadwinner", "noreast", "furacious", "epauliere", "omniscribent"
			, "Passiflorales", "uninductive", "inductivity", "Orbitolina", "Semecarpus", "migrainoid"
			, "steprelationship", "phlogisticate", "mesymnion", "sloped", "edificator", "beneficent"
			, "culm", "paleornithology", "unurban", "throbless", "amplexifoliate", "sesquiquintile"
			, "sapience", "astucious", "dithery", "boor", "ambitus", "scotching", "uloid", "uncompromisingness"
			, "hoove", "waird", "marshiness", "Jerusalem", "mericarp", "unevoked", "benzoperoxide"
			, "outguess", "pyxie", "hymnic", "euphemize", "mendacity", "erythremia", "rosaniline"
			, "unchatteled", "lienteria", "Bushongo", "dialoguer", "unrepealably", "rivethead"
			, "antideflation", "vinegarish", "manganosiderite", "doubtingness", "ovopyriform"
			, "Cephalodiscus", "Muscicapa", "Animalivora", "angina", "planispheric", "ipomoein"
			, "cuproiodargyrite", "sandbox", "scrat", "Munnopsidae", "shola", "pentafid", "overstudiousness"
			, "times", "nonprofession", "appetible", "valvulotomy", "goladar", "uniarticular"
			, "oxyterpene", "unlapsing", "omega", "trophonema", "seminonflammable", "circumzenithal"
			, "starer", "depthwise", "liberatress", "unleavened", "unrevolting", "groundneedle"
			, "topline", "wandoo", "umangite", "ordinant", "unachievable", "oversand", "snare"
			, "avengeful", "unexplicit", "mustafina", "sonable", "rehabilitative", "eulogization"
			, "papery", "technopsychology", "impressor", "cresylite", "entame", "transudatory"
			, "scotale", "pachydermatoid", "imaginary", "yeat", "slipped", "stewardship", "adatom"
			, "cockstone", "skyshine", "heavenful", "comparability", "exprobratory", "dermorhynchous"
			, "parquet", "cretaceous", "vesperal", "raphis", "undangered", "Glecoma", "engrain"
			, "counteractively", "Zuludom", "orchiocatabasis", "Auriculariales", "warriorwise"
			, "extraorganismal", "overbuilt", "alveolite", "tetchy", "terrificness", "widdle"
			, "unpremonished", "rebilling", "sequestrum", "equiconvex", "heliocentricism", "catabaptist"
			, "okonite", "propheticism", "helminthagogic", "calycular", "giantly", "wingable"
			, "golem", "unprovided", "commandingness", "greave", "haply", "doina", "depressingly"
			, "subdentate", "impairment", "decidable", "neurotrophic", "unpredict", "bicorporeal"
			, "pendulant", "flatman", "intrabred", "toplike", "Prosobranchiata", "farrantly"
			, "toxoplasmosis", "gorilloid", "dipsomaniacal", "aquiline", "atlantite", "ascitic"
			, "perculsive", "prospectiveness", "saponaceous", "centrifugalization", "dinical"
			, "infravaginal", "beadroll", "affaite", "Helvidian", "tickleproof", "abstractionism"
			, "enhedge", "outwealth", "overcontribute", "coldfinch", "gymnastic", "Pincian", 
			"Munychian", "codisjunct", "quad", "coracomandibular", "phoenicochroite", "amender"
			, "selectivity", "putative", "semantician", "lophotrichic", "Spatangoidea", "saccharogenic"
			, "inferent", "Triconodonta", "arrendation", "sheepskin", "taurocolla", "bunghole"
			, "Machiavel", "triakistetrahedral", "dehairer", "prezygapophysial", "cylindric"
			, "pneumonalgia", "sleigher", "emir", "Socraticism", "licitness", "massedly", "instructiveness"
			, "sturdied", "redecrease", "starosta", "evictor", "orgiastic", "squdge", "meloplasty"
			, "Tsonecan", "repealableness", "swoony", "myesthesia", "molecule", "autobiographist"
			, "reciprocation", "refective", "unobservantness", "tricae", "ungouged", "floatability"
			, "Mesua", "fetlocked", "chordacentrum", "sedentariness", "various", "laubanite"
			, "nectopod", "zenick", "sequentially", "analgic", "biodynamics", "posttraumatic"
			, "nummi", "pyroacetic", "bot", "redescend", "dispermy", "undiffusive", "circular"
			, "trillion", "Uraniidae", "ploration", "discipular", "potentness", "sud", "Hu", 
			"Eryon", "plugger", "subdrainage", "jharal", "abscission", "supermarket", "countergabion"
			, "glacierist", "lithotresis", "minniebush", "zanyism", "eucalypteol", "sterilely"
			, "unrealize", "unpatched", "hypochondriacism", "critically", "cheesecutter" };
	}
}
