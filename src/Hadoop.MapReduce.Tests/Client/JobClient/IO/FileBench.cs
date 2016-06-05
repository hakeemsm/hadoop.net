using System;
using System.Collections.Generic;
using System.IO;
using System.Text;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.IO.Compress;
using Org.Apache.Hadoop.Mapred;
using Org.Apache.Hadoop.Util;
using Sharpen;

namespace Org.Apache.Hadoop.IO
{
	public class FileBench : Configured, Tool
	{
		internal static int PrintUsage()
		{
			ToolRunner.PrintGenericCommandUsage(System.Console.Out);
			System.Console.Out.WriteLine("Usage: Task list:           -[no]r -[no]w\n" + "       Format:              -[no]seq -[no]txt\n"
				 + "       CompressionCodec:    -[no]zip -[no]pln\n" + "       CompressionType:     -[no]blk -[no]rec\n"
				 + "       Required:            -dir <working dir>\n" + "All valid combinations are implicitly enabled, unless an option is enabled\n"
				 + "explicitly. For example, specifying \"-zip\", excludes -pln,\n" + "unless they are also explicitly included, as in \"-pln -zip\"\n"
				 + "Note that CompressionType params only apply to SequenceFiles\n\n" + "Useful options to set:\n"
				 + "-D fs.defaultFS=\"file:///\" \\\n" + "-D fs.file.impl=org.apache.hadoop.fs.RawLocalFileSystem \\\n"
				 + "-D filebench.file.bytes=$((10*1024*1024*1024)) \\\n" + "-D filebench.key.words=5 \\\n"
				 + "-D filebench.val.words=20\n");
			return -1;
		}

		internal static string[] keys;

		internal static string[] values;

		internal static StringBuilder sentence = new StringBuilder();

		private static string GenerateSentence(Random r, int noWords)
		{
			sentence.Length = 0;
			for (int i = 0; i < noWords; ++i)
			{
				sentence.Append(words[r.Next(words.Length)]);
				sentence.Append(" ");
			}
			return sentence.ToString();
		}

		// fill keys, values with ~1.5 blocks for block-compressed seq fill
		private static void FillBlocks(JobConf conf)
		{
			Random r = new Random();
			long seed = conf.GetLong("filebench.seed", -1);
			if (seed > 0)
			{
				r.SetSeed(seed);
			}
			int keylen = conf.GetInt("filebench.key.words", 5);
			int vallen = conf.GetInt("filebench.val.words", 20);
			int acc = (3 * conf.GetInt("io.seqfile.compress.blocksize", 1000000)) >> 1;
			AList<string> k = new AList<string>();
			AList<string> v = new AList<string>();
			for (int i = 0; acc > 0; ++i)
			{
				string s = GenerateSentence(r, keylen);
				acc -= s.Length;
				k.AddItem(s);
				s = GenerateSentence(r, vallen);
				acc -= s.Length;
				v.AddItem(s);
			}
			keys = Sharpen.Collections.ToArray(k, new string[0]);
			values = Sharpen.Collections.ToArray(v, new string[0]);
		}

		/// <exception cref="System.IO.IOException"/>
		internal static long WriteBench(JobConf conf)
		{
			// OutputFormat instantiation
			long filelen = conf.GetLong("filebench.file.bytes", 5 * 1024 * 1024 * 1024);
			Org.Apache.Hadoop.IO.Text key = new Org.Apache.Hadoop.IO.Text();
			Org.Apache.Hadoop.IO.Text val = new Org.Apache.Hadoop.IO.Text();
			string fn = conf.Get("test.filebench.name", string.Empty);
			Path outd = FileOutputFormat.GetOutputPath(conf);
			conf.Set("mapred.work.output.dir", outd.ToString());
			OutputFormat outf = conf.GetOutputFormat();
			RecordWriter<Org.Apache.Hadoop.IO.Text, Org.Apache.Hadoop.IO.Text> rw = outf.GetRecordWriter
				(outd.GetFileSystem(conf), conf, fn, Reporter.Null);
			try
			{
				long acc = 0L;
				DateTime start = new DateTime();
				for (int i = 0; acc < filelen; ++i)
				{
					i %= keys.Length;
					key.Set(keys[i]);
					val.Set(values[i]);
					rw.Write(key, val);
					acc += keys[i].Length;
					acc += values[i].Length;
				}
				DateTime end = new DateTime();
				return end.GetTime() - start.GetTime();
			}
			finally
			{
				rw.Close(Reporter.Null);
			}
		}

		/// <exception cref="System.IO.IOException"/>
		internal static long ReadBench(JobConf conf)
		{
			// InputFormat instantiation
			InputFormat inf = conf.GetInputFormat();
			string fn = conf.Get("test.filebench.name", string.Empty);
			Path pin = new Path(FileInputFormat.GetInputPaths(conf)[0], fn);
			FileStatus @in = pin.GetFileSystem(conf).GetFileStatus(pin);
			RecordReader rr = inf.GetRecordReader(new FileSplit(pin, 0, @in.GetLen(), (string
				[])null), conf, Reporter.Null);
			try
			{
				object key = rr.CreateKey();
				object val = rr.CreateValue();
				DateTime start = new DateTime();
				while (rr.Next(key, val))
				{
				}
				DateTime end = new DateTime();
				return end.GetTime() - start.GetTime();
			}
			finally
			{
				rr.Close();
			}
		}

		/// <exception cref="System.Exception"/>
		public static void Main(string[] args)
		{
			int res = ToolRunner.Run(new Configuration(), new FileBench(), args);
			System.Environment.Exit(res);
		}

		/// <summary>Process params from command line and run set of benchmarks specified.</summary>
		/// <exception cref="System.IO.IOException"/>
		public virtual int Run(string[] argv)
		{
			JobConf job = new JobConf(GetConf());
			EnumSet<FileBench.CCodec> cc = null;
			EnumSet<FileBench.CType> ct = null;
			EnumSet<FileBench.Format> f = null;
			EnumSet<FileBench.RW> rw = null;
			Path root = null;
			FileSystem fs = FileSystem.Get(job);
			for (int i = 0; i < argv.Length; ++i)
			{
				try
				{
					if ("-dir".Equals(argv[i]))
					{
						root = new Path(argv[++i]).MakeQualified(fs);
						System.Console.Out.WriteLine("DIR: " + root.ToString());
					}
					else
					{
						if ("-seed".Equals(argv[i]))
						{
							job.SetLong("filebench.seed", Sharpen.Extensions.ValueOf(argv[++i]));
						}
						else
						{
							if (argv[i].StartsWith("-no"))
							{
								string arg = Sharpen.Runtime.Substring(argv[i], 3);
								cc = Rem<FileBench.CCodec>(cc, arg);
								ct = Rem<FileBench.CType>(ct, arg);
								f = Rem<FileBench.Format>(f, arg);
								rw = Rem<FileBench.RW>(rw, arg);
							}
							else
							{
								string arg = Sharpen.Runtime.Substring(argv[i], 1);
								cc = Add<FileBench.CCodec>(cc, arg);
								ct = Add<FileBench.CType>(ct, arg);
								f = Add<FileBench.Format>(f, arg);
								rw = Add<FileBench.RW>(rw, arg);
							}
						}
					}
				}
				catch (Exception e)
				{
					throw (IOException)Sharpen.Extensions.InitCause(new IOException(), e);
				}
			}
			if (null == root)
			{
				System.Console.Out.WriteLine("Missing -dir param");
				PrintUsage();
				return -1;
			}
			FillBlocks(job);
			job.SetOutputKeyClass(typeof(Org.Apache.Hadoop.IO.Text));
			job.SetOutputValueClass(typeof(Org.Apache.Hadoop.IO.Text));
			FileInputFormat.SetInputPaths(job, root);
			FileOutputFormat.SetOutputPath(job, root);
			if (null == cc)
			{
				cc = EnumSet.AllOf<FileBench.CCodec>();
			}
			if (null == ct)
			{
				ct = EnumSet.AllOf<FileBench.CType>();
			}
			if (null == f)
			{
				f = EnumSet.AllOf<FileBench.Format>();
			}
			if (null == rw)
			{
				rw = EnumSet.AllOf<FileBench.RW>();
			}
			foreach (FileBench.RW rwop in rw)
			{
				foreach (FileBench.Format fmt in f)
				{
					fmt.Configure(job);
					foreach (FileBench.CCodec cod in cc)
					{
						cod.Configure(job);
						if (!(fmt == FileBench.Format.txt || cod == FileBench.CCodec.pln))
						{
							foreach (FileBench.CType typ in ct)
							{
								string fn = StringUtils.ToUpperCase(fmt.ToString()) + "_" + StringUtils.ToUpperCase
									(cod.ToString()) + "_" + StringUtils.ToUpperCase(typ.ToString());
								typ.Configure(job);
								System.Console.Out.Write(StringUtils.ToUpperCase(rwop.ToString()) + " " + fn + ": "
									);
								System.Console.Out.WriteLine(rwop.Exec(fn, job) / 1000 + " seconds");
							}
						}
						else
						{
							string fn = StringUtils.ToUpperCase(fmt.ToString()) + "_" + StringUtils.ToUpperCase
								(cod.ToString());
							Path p = new Path(root, fn);
							if (rwop == FileBench.RW.r && !fs.Exists(p))
							{
								fn += cod.GetExt();
							}
							System.Console.Out.Write(StringUtils.ToUpperCase(rwop.ToString()) + " " + fn + ": "
								);
							System.Console.Out.WriteLine(rwop.Exec(fn, job) / 1000 + " seconds");
						}
					}
				}
			}
			return 0;
		}

		[System.Serializable]
		internal sealed class CCodec
		{
			public static readonly FileBench.CCodec zip = new FileBench.CCodec(typeof(GzipCodec
				), ".gz");

			public static readonly FileBench.CCodec pln = new FileBench.CCodec(null, string.Empty
				);

			internal Type inf;

			internal string ext;

			internal CCodec(Type inf, string ext)
			{
				// overwrought argument processing and wordlist follow
				this.inf = inf;
				this.ext = ext;
			}

			public void Configure(JobConf job)
			{
				if (FileBench.CCodec.inf != null)
				{
					job.SetBoolean("mapred.output.compress", true);
					job.SetClass("mapred.output.compression.codec", FileBench.CCodec.inf, typeof(CompressionCodec
						));
				}
				else
				{
					job.SetBoolean("mapred.output.compress", false);
				}
			}

			public string GetExt()
			{
				return FileBench.CCodec.ext;
			}
		}

		[System.Serializable]
		internal sealed class CType
		{
			public static readonly FileBench.CType blk = new FileBench.CType("BLOCK");

			public static readonly FileBench.CType rec = new FileBench.CType("RECORD");

			internal string typ;

			internal CType(string typ)
			{
				this.typ = typ;
			}

			public void Configure(JobConf job)
			{
				job.Set("mapred.map.output.compression.type", FileBench.CType.typ);
				job.Set("mapred.output.compression.type", FileBench.CType.typ);
			}
		}

		[System.Serializable]
		internal sealed class Format
		{
			public static readonly FileBench.Format seq = new FileBench.Format(typeof(SequenceFileInputFormat
				), typeof(SequenceFileOutputFormat));

			public static readonly FileBench.Format txt = new FileBench.Format(typeof(TextInputFormat
				), typeof(TextOutputFormat));

			internal Type inf;

			internal Type of;

			internal Format(Type inf, Type of)
			{
				this.inf = inf;
				this.of = of;
			}

			public void Configure(JobConf job)
			{
				if (null != FileBench.Format.inf)
				{
					job.SetInputFormat(FileBench.Format.inf);
				}
				if (null != FileBench.Format.of)
				{
					job.SetOutputFormat(FileBench.Format.of);
				}
			}
		}

		[System.Serializable]
		internal sealed class RW
		{
			public static readonly FileBench.RW w = new FileBench.RW();

			public static readonly FileBench.RW r = new FileBench.RW();

			/// <exception cref="System.IO.IOException"/>
			public abstract long Exec(string fn, JobConf job);
		}

		internal static IDictionary<Type, IDictionary<string, Enum>> fullmap = new Dictionary
			<Type, IDictionary<string, Enum>>();

		static FileBench()
		{
			// can't effectively use Enum::valueOf
			IDictionary<string, FileBench.CCodec> m1 = new Dictionary<string, FileBench.CCodec
				>();
			foreach (FileBench.CCodec v in FileBench.CCodec.Values())
			{
				m1[v.ToString()] = v;
			}
			fullmap[typeof(FileBench.CCodec)] = m1;
			IDictionary<string, FileBench.CType> m2 = new Dictionary<string, FileBench.CType>
				();
			foreach (FileBench.CType v_1 in FileBench.CType.Values())
			{
				m2[v_1.ToString()] = v_1;
			}
			fullmap[typeof(FileBench.CType)] = m2;
			IDictionary<string, FileBench.Format> m3 = new Dictionary<string, FileBench.Format
				>();
			foreach (FileBench.Format v_2 in FileBench.Format.Values())
			{
				m3[v_2.ToString()] = v_2;
			}
			fullmap[typeof(FileBench.Format)] = m3;
			IDictionary<string, FileBench.RW> m4 = new Dictionary<string, FileBench.RW>();
			foreach (FileBench.RW v_3 in FileBench.RW.Values())
			{
				m4[v_3.ToString()] = v_3;
			}
			fullmap[typeof(FileBench.RW)] = m4;
		}

		public static EnumSet<T> Rem<T>(EnumSet<T> set, string s)
			where T : Enum<T>
		{
			System.Type c = typeof(T);
			if (null != fullmap[c] && fullmap[c][s] != null)
			{
				if (null == set)
				{
					set = EnumSet.AllOf(c);
				}
				set.Remove(fullmap[c][s]);
			}
			return set;
		}

		public static EnumSet<T> Add<T>(EnumSet<T> set, string s)
			where T : Enum<T>
		{
			System.Type c = typeof(T);
			if (null != fullmap[c] && fullmap[c][s] != null)
			{
				if (null == set)
				{
					set = EnumSet.NoneOf(c);
				}
				set.AddItem((T)fullmap[c][s]);
			}
			return set;
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
