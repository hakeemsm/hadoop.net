using System;
using System.Collections.Generic;
using System.Text;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.IO;
using Org.Apache.Hadoop.Mapred;
using Org.Apache.Hadoop.Mapreduce;
using Org.Apache.Hadoop.Mapreduce.Lib.Output;
using Org.Apache.Hadoop.Util;
using Sharpen;

namespace Org.Apache.Hadoop.Examples
{
	/// <summary>
	/// This program uses map/reduce to just run a distributed job where there is
	/// no interaction between the tasks and each task writes a large unsorted
	/// random sequence of words.
	/// </summary>
	/// <remarks>
	/// This program uses map/reduce to just run a distributed job where there is
	/// no interaction between the tasks and each task writes a large unsorted
	/// random sequence of words.
	/// In order for this program to generate data for terasort with a 5-10 words
	/// per key and 20-100 words per value, have the following config:
	/// <pre>
	/// <c>
	/// &lt;?xml version="1.0"?&gt;
	/// &lt;?xml-stylesheet type="text/xsl" href="configuration.xsl"?&gt;
	/// &lt;configuration&gt;
	/// &lt;property&gt;
	/// &lt;name&gt;mapreduce.randomtextwriter.minwordskey&lt;/name&gt;
	/// &lt;value&gt;5&lt;/value&gt;
	/// &lt;/property&gt;
	/// &lt;property&gt;
	/// &lt;name&gt;mapreduce.randomtextwriter.maxwordskey&lt;/name&gt;
	/// &lt;value&gt;10&lt;/value&gt;
	/// &lt;/property&gt;
	/// &lt;property&gt;
	/// &lt;name&gt;mapreduce.randomtextwriter.minwordsvalue&lt;/name&gt;
	/// &lt;value&gt;20&lt;/value&gt;
	/// &lt;/property&gt;
	/// &lt;property&gt;
	/// &lt;name&gt;mapreduce.randomtextwriter.maxwordsvalue&lt;/name&gt;
	/// &lt;value&gt;100&lt;/value&gt;
	/// &lt;/property&gt;
	/// &lt;property&gt;
	/// &lt;name&gt;mapreduce.randomtextwriter.totalbytes&lt;/name&gt;
	/// &lt;value&gt;1099511627776&lt;/value&gt;
	/// &lt;/property&gt;
	/// &lt;/configuration&gt;
	/// </c>
	/// </pre>
	/// Equivalently,
	/// <see cref="RandomTextWriter"/>
	/// also supports all the above options
	/// and ones supported by
	/// <see cref="Org.Apache.Hadoop.Util.Tool"/>
	/// via the command-line.
	/// To run: bin/hadoop jar hadoop-${version}-examples.jar randomtextwriter
	/// [-outFormat <i>output format class</i>] <i>output</i>
	/// </remarks>
	public class RandomTextWriter : Configured, Tool
	{
		public const string TotalBytes = "mapreduce.randomtextwriter.totalbytes";

		public const string BytesPerMap = "mapreduce.randomtextwriter.bytespermap";

		public const string MapsPerHost = "mapreduce.randomtextwriter.mapsperhost";

		public const string MaxValue = "mapreduce.randomtextwriter.maxwordsvalue";

		public const string MinValue = "mapreduce.randomtextwriter.minwordsvalue";

		public const string MinKey = "mapreduce.randomtextwriter.minwordskey";

		public const string MaxKey = "mapreduce.randomtextwriter.maxwordskey";

		internal static int PrintUsage()
		{
			System.Console.Out.WriteLine("randomtextwriter " + "[-outFormat <output format class>] "
				 + "<output>");
			ToolRunner.PrintGenericCommandUsage(System.Console.Out);
			return 2;
		}

		/// <summary>User counters</summary>
		internal enum Counters
		{
			RecordsWritten,
			BytesWritten
		}

		internal class RandomTextMapper : Mapper<Text, Text, Text, Text>
		{
			private long numBytesToWrite;

			private int minWordsInKey;

			private int wordsInKeyRange;

			private int minWordsInValue;

			private int wordsInValueRange;

			private Random random = new Random();

			/// <summary>Save the configuration value that we need to write the data.</summary>
			protected override void Setup(Mapper.Context context)
			{
				Configuration conf = context.GetConfiguration();
				numBytesToWrite = conf.GetLong(BytesPerMap, 1 * 1024 * 1024 * 1024);
				minWordsInKey = conf.GetInt(MinKey, 5);
				wordsInKeyRange = (conf.GetInt(MaxKey, 10) - minWordsInKey);
				minWordsInValue = conf.GetInt(MinValue, 10);
				wordsInValueRange = (conf.GetInt(MaxValue, 100) - minWordsInValue);
			}

			/// <summary>Given an output filename, write a bunch of random records to it.</summary>
			/// <exception cref="System.IO.IOException"/>
			/// <exception cref="System.Exception"/>
			protected override void Map(Text key, Text value, Mapper.Context context)
			{
				int itemCount = 0;
				while (numBytesToWrite > 0)
				{
					// Generate the key/value 
					int noWordsKey = minWordsInKey + (wordsInKeyRange != 0 ? random.Next(wordsInKeyRange
						) : 0);
					int noWordsValue = minWordsInValue + (wordsInValueRange != 0 ? random.Next(wordsInValueRange
						) : 0);
					Text keyWords = GenerateSentence(noWordsKey);
					Text valueWords = GenerateSentence(noWordsValue);
					// Write the sentence 
					context.Write(keyWords, valueWords);
					numBytesToWrite -= (keyWords.GetLength() + valueWords.GetLength());
					// Update counters, progress etc.
					context.GetCounter(RandomTextWriter.Counters.BytesWritten).Increment(keyWords.GetLength
						() + valueWords.GetLength());
					context.GetCounter(RandomTextWriter.Counters.RecordsWritten).Increment(1);
					if (++itemCount % 200 == 0)
					{
						context.SetStatus("wrote record " + itemCount + ". " + numBytesToWrite + " bytes left."
							);
					}
				}
				context.SetStatus("done with " + itemCount + " records.");
			}

			private Text GenerateSentence(int noWords)
			{
				StringBuilder sentence = new StringBuilder();
				string space = " ";
				for (int i = 0; i < noWords; ++i)
				{
					sentence.Append(words[random.Next(words.Length)]);
					sentence.Append(space);
				}
				return new Org.Apache.Hadoop.IO.Text(sentence.ToString());
			}
		}

		/// <summary>This is the main routine for launching a distributed random write job.</summary>
		/// <remarks>
		/// This is the main routine for launching a distributed random write job.
		/// It runs 10 maps/node and each node writes 1 gig of data to a DFS file.
		/// The reduce doesn't do anything.
		/// </remarks>
		/// <exception cref="System.IO.IOException"></exception>
		/// <exception cref="System.Exception"/>
		public virtual int Run(string[] args)
		{
			if (args.Length == 0)
			{
				return PrintUsage();
			}
			Configuration conf = GetConf();
			JobClient client = new JobClient(conf);
			ClusterStatus cluster = client.GetClusterStatus();
			int numMapsPerHost = conf.GetInt(MapsPerHost, 10);
			long numBytesToWritePerMap = conf.GetLong(BytesPerMap, 1 * 1024 * 1024 * 1024);
			if (numBytesToWritePerMap == 0)
			{
				System.Console.Error.WriteLine("Cannot have " + BytesPerMap + " set to 0");
				return -2;
			}
			long totalBytesToWrite = conf.GetLong(TotalBytes, numMapsPerHost * numBytesToWritePerMap
				 * cluster.GetTaskTrackers());
			int numMaps = (int)(totalBytesToWrite / numBytesToWritePerMap);
			if (numMaps == 0 && totalBytesToWrite > 0)
			{
				numMaps = 1;
				conf.SetLong(BytesPerMap, totalBytesToWrite);
			}
			conf.SetInt(MRJobConfig.NumMaps, numMaps);
			Job job = Job.GetInstance(conf);
			job.SetJarByClass(typeof(RandomTextWriter));
			job.SetJobName("random-text-writer");
			job.SetOutputKeyClass(typeof(Org.Apache.Hadoop.IO.Text));
			job.SetOutputValueClass(typeof(Org.Apache.Hadoop.IO.Text));
			job.SetInputFormatClass(typeof(RandomWriter.RandomInputFormat));
			job.SetMapperClass(typeof(RandomTextWriter.RandomTextMapper));
			Type outputFormatClass = typeof(SequenceFileOutputFormat);
			IList<string> otherArgs = new AList<string>();
			for (int i = 0; i < args.Length; ++i)
			{
				try
				{
					if ("-outFormat".Equals(args[i]))
					{
						outputFormatClass = Sharpen.Runtime.GetType(args[++i]).AsSubclass<OutputFormat>();
					}
					else
					{
						otherArgs.AddItem(args[i]);
					}
				}
				catch (IndexOutOfRangeException)
				{
					System.Console.Out.WriteLine("ERROR: Required parameter missing from " + args[i -
						 1]);
					return PrintUsage();
				}
			}
			// exits
			job.SetOutputFormatClass(outputFormatClass);
			FileOutputFormat.SetOutputPath(job, new Path(otherArgs[0]));
			System.Console.Out.WriteLine("Running " + numMaps + " maps.");
			// reducer NONE
			job.SetNumReduceTasks(0);
			DateTime startTime = new DateTime();
			System.Console.Out.WriteLine("Job started: " + startTime);
			int ret = job.WaitForCompletion(true) ? 0 : 1;
			DateTime endTime = new DateTime();
			System.Console.Out.WriteLine("Job ended: " + endTime);
			System.Console.Out.WriteLine("The job took " + (endTime.GetTime() - startTime.GetTime
				()) / 1000 + " seconds.");
			return ret;
		}

		/// <exception cref="System.Exception"/>
		public static void Main(string[] args)
		{
			int res = ToolRunner.Run(new Configuration(), new RandomTextWriter(), args);
			System.Environment.Exit(res);
		}

		/// <summary>A random list of 1000 words from /usr/share/dict/words</summary>
		private static string[] words = new string[] { "diurnalness", "Homoiousian", "spiranthic"
			, "tetragynian", "silverhead", "ungreat", "lithograph", "exploiter", "physiologian"
			, "by", "hellbender", "Filipendula", "undeterring", "antiscolic", "pentagamist", 
			"hypoid", "cacuminal", "sertularian", "schoolmasterism", "nonuple", "gallybeggar"
			, "phytonic", "swearingly", "nebular", "Confervales", "thermochemically", "characinoid"
			, "cocksuredom", "fallacious", "feasibleness", "debromination", "playfellowship"
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
