/*
* Licensed to the Apache Software Foundation (ASF) under one
* or more contributor license agreements.  See the NOTICE file
* distributed with this work for additional information
* regarding copyright ownership.  The ASF licenses this file
* to you under the Apache License, Version 2.0 (the
* "License"); you may not use this file except in compliance
* with the License.  You may obtain a copy of the License at
*
*     http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
*/
using System;
using System.Collections.Generic;
using System.IO;
using System.Text;
using Hadoop.Common.Core.Conf;
using Hadoop.Common.Core.Util;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.Util;


namespace Org.Apache.Hadoop.IO.Compress
{
	/// <summary>A factory that will find the correct codec for a given filename.</summary>
	public class CompressionCodecFactory
	{
		public static readonly Log Log = LogFactory.GetLog(typeof(Org.Apache.Hadoop.IO.Compress.CompressionCodecFactory
			).FullName);

		private static readonly ServiceLoader<CompressionCodec> CodecProviders = ServiceLoader
			.Load<CompressionCodec>();

		/// <summary>A map from the reversed filename suffixes to the codecs.</summary>
		/// <remarks>
		/// A map from the reversed filename suffixes to the codecs.
		/// This is probably overkill, because the maps should be small, but it
		/// automatically supports finding the longest matching suffix.
		/// </remarks>
		private SortedDictionary<string, CompressionCodec> codecs = null;

		/// <summary>A map from the reversed filename suffixes to the codecs.</summary>
		/// <remarks>
		/// A map from the reversed filename suffixes to the codecs.
		/// This is probably overkill, because the maps should be small, but it
		/// automatically supports finding the longest matching suffix.
		/// </remarks>
		private IDictionary<string, CompressionCodec> codecsByName = null;

		/// <summary>A map from class names to the codecs</summary>
		private Dictionary<string, CompressionCodec> codecsByClassName = null;

		private void AddCodec(CompressionCodec codec)
		{
			string suffix = codec.GetDefaultExtension();
			codecs[((StringBuilder)new StringBuilder(suffix).Reverse()).ToString()] = codec;
			codecsByClassName[codec.GetType().GetCanonicalName()] = codec;
			string codecName = codec.GetType().Name;
			codecsByName[StringUtils.ToLowerCase(codecName)] = codec;
			if (codecName.EndsWith("Codec"))
			{
				codecName = Runtime.Substring(codecName, 0, codecName.Length - "Codec".Length
					);
				codecsByName[StringUtils.ToLowerCase(codecName)] = codec;
			}
		}

		/// <summary>Print the extension map out as a string.</summary>
		public override string ToString()
		{
			StringBuilder buf = new StringBuilder();
			IEnumerator<KeyValuePair<string, CompressionCodec>> itr = codecs.GetEnumerator();
			buf.Append("{ ");
			if (itr.HasNext())
			{
				KeyValuePair<string, CompressionCodec> entry = itr.Next();
				buf.Append(entry.Key);
				buf.Append(": ");
				buf.Append(entry.Value.GetType().FullName);
				while (itr.HasNext())
				{
					entry = itr.Next();
					buf.Append(", ");
					buf.Append(entry.Key);
					buf.Append(": ");
					buf.Append(entry.Value.GetType().FullName);
				}
			}
			buf.Append(" }");
			return buf.ToString();
		}

		/// <summary>
		/// Get the list of codecs discovered via a Java ServiceLoader, or
		/// listed in the configuration.
		/// </summary>
		/// <remarks>
		/// Get the list of codecs discovered via a Java ServiceLoader, or
		/// listed in the configuration. Codecs specified in configuration come
		/// later in the returned list, and are considered to override those
		/// from the ServiceLoader.
		/// </remarks>
		/// <param name="conf">the configuration to look in</param>
		/// <returns>
		/// a list of the
		/// <see cref="CompressionCodec"/>
		/// classes
		/// </returns>
		public static IList<Type> GetCodecClasses(Configuration conf)
		{
			IList<Type> result = new AList<Type>();
			// Add codec classes discovered via service loading
			lock (CodecProviders)
			{
				// CODEC_PROVIDERS is a lazy collection. Synchronize so it is
				// thread-safe. See HADOOP-8406.
				foreach (CompressionCodec codec in CodecProviders)
				{
					result.AddItem(codec.GetType());
				}
			}
			// Add codec classes from configuration
			string codecsString = conf.Get(CommonConfigurationKeys.IoCompressionCodecsKey);
			if (codecsString != null)
			{
				StringTokenizer codecSplit = new StringTokenizer(codecsString, ",");
				while (codecSplit.MoveNext())
				{
					string codecSubstring = codecSplit.NextToken().Trim();
					if (codecSubstring.Length != 0)
					{
						try
						{
							Type cls = conf.GetClassByName(codecSubstring);
							if (!typeof(CompressionCodec).IsAssignableFrom(cls))
							{
								throw new ArgumentException("Class " + codecSubstring + " is not a CompressionCodec"
									);
							}
							result.AddItem(cls.AsSubclass<CompressionCodec>());
						}
						catch (TypeLoadException ex)
						{
							throw new ArgumentException("Compression codec " + codecSubstring + " not found."
								, ex);
						}
					}
				}
			}
			return result;
		}

		/// <summary>Sets a list of codec classes in the configuration.</summary>
		/// <remarks>
		/// Sets a list of codec classes in the configuration. In addition to any
		/// classes specified using this method,
		/// <see cref="CompressionCodec"/>
		/// classes on
		/// the classpath are discovered using a Java ServiceLoader.
		/// </remarks>
		/// <param name="conf">the configuration to modify</param>
		/// <param name="classes">the list of classes to set</param>
		public static void SetCodecClasses(Configuration conf, IList<Type> classes)
		{
			StringBuilder buf = new StringBuilder();
			IEnumerator<Type> itr = classes.GetEnumerator();
			if (itr.HasNext())
			{
				Type cls = itr.Next();
				buf.Append(cls.FullName);
				while (itr.HasNext())
				{
					buf.Append(',');
					buf.Append(itr.Next().FullName);
				}
			}
			conf.Set(CommonConfigurationKeys.IoCompressionCodecsKey, buf.ToString());
		}

		/// <summary>
		/// Find the codecs specified in the config value io.compression.codecs
		/// and register them.
		/// </summary>
		/// <remarks>
		/// Find the codecs specified in the config value io.compression.codecs
		/// and register them. Defaults to gzip and deflate.
		/// </remarks>
		public CompressionCodecFactory(Configuration conf)
		{
			codecs = new SortedDictionary<string, CompressionCodec>();
			codecsByClassName = new Dictionary<string, CompressionCodec>();
			codecsByName = new Dictionary<string, CompressionCodec>();
			IList<Type> codecClasses = GetCodecClasses(conf);
			if (codecClasses == null || codecClasses.IsEmpty())
			{
				AddCodec(new GzipCodec());
				AddCodec(new DefaultCodec());
			}
			else
			{
				foreach (Type codecClass in codecClasses)
				{
					AddCodec(ReflectionUtils.NewInstance(codecClass, conf));
				}
			}
		}

		/// <summary>
		/// Find the relevant compression codec for the given file based on its
		/// filename suffix.
		/// </summary>
		/// <param name="file">the filename to check</param>
		/// <returns>the codec object</returns>
		public virtual CompressionCodec GetCodec(Path file)
		{
			CompressionCodec result = null;
			if (codecs != null)
			{
				string filename = file.GetName();
				string reversedFilename = ((StringBuilder)new StringBuilder(filename).Reverse()).
					ToString();
				SortedDictionary<string, CompressionCodec> subMap = codecs.HeadMap(reversedFilename
					);
				if (!subMap.IsEmpty())
				{
					string potentialSuffix = subMap.LastKey();
					if (reversedFilename.StartsWith(potentialSuffix))
					{
						result = codecs[potentialSuffix];
					}
				}
			}
			return result;
		}

		/// <summary>Find the relevant compression codec for the codec's canonical class name.
		/// 	</summary>
		/// <param name="classname">the canonical class name of the codec</param>
		/// <returns>the codec object</returns>
		public virtual CompressionCodec GetCodecByClassName(string classname)
		{
			if (codecsByClassName == null)
			{
				return null;
			}
			return codecsByClassName[classname];
		}

		/// <summary>
		/// Find the relevant compression codec for the codec's canonical class name
		/// or by codec alias.
		/// </summary>
		/// <remarks>
		/// Find the relevant compression codec for the codec's canonical class name
		/// or by codec alias.
		/// <p/>
		/// Codec aliases are case insensitive.
		/// <p/>
		/// The code alias is the short class name (without the package name).
		/// If the short class name ends with 'Codec', then there are two aliases for
		/// the codec, the complete short class name and the short class name without
		/// the 'Codec' ending. For example for the 'GzipCodec' codec class name the
		/// alias are 'gzip' and 'gzipcodec'.
		/// </remarks>
		/// <param name="codecName">the canonical class name of the codec</param>
		/// <returns>the codec object</returns>
		public virtual CompressionCodec GetCodecByName(string codecName)
		{
			if (codecsByClassName == null)
			{
				return null;
			}
			CompressionCodec codec = GetCodecByClassName(codecName);
			if (codec == null)
			{
				// trying to get the codec by name in case the name was specified
				// instead a class
				codec = codecsByName[StringUtils.ToLowerCase(codecName)];
			}
			return codec;
		}

		/// <summary>
		/// Find the relevant compression codec for the codec's canonical class name
		/// or by codec alias and returns its implemetation class.
		/// </summary>
		/// <remarks>
		/// Find the relevant compression codec for the codec's canonical class name
		/// or by codec alias and returns its implemetation class.
		/// <p/>
		/// Codec aliases are case insensitive.
		/// <p/>
		/// The code alias is the short class name (without the package name).
		/// If the short class name ends with 'Codec', then there are two aliases for
		/// the codec, the complete short class name and the short class name without
		/// the 'Codec' ending. For example for the 'GzipCodec' codec class name the
		/// alias are 'gzip' and 'gzipcodec'.
		/// </remarks>
		/// <param name="codecName">the canonical class name of the codec</param>
		/// <returns>the codec class</returns>
		public virtual Type GetCodecClassByName(string codecName)
		{
			CompressionCodec codec = GetCodecByName(codecName);
			if (codec == null)
			{
				return null;
			}
			return codec.GetType();
		}

		/// <summary>Removes a suffix from a filename, if it has it.</summary>
		/// <param name="filename">the filename to strip</param>
		/// <param name="suffix">the suffix to remove</param>
		/// <returns>the shortened filename</returns>
		public static string RemoveSuffix(string filename, string suffix)
		{
			if (filename.EndsWith(suffix))
			{
				return Runtime.Substring(filename, 0, filename.Length - suffix.Length);
			}
			return filename;
		}

		/// <summary>A little test program.</summary>
		/// <param name="args"/>
		/// <exception cref="System.Exception"/>
		public static void Main(string[] args)
		{
			Configuration conf = new Configuration();
			Org.Apache.Hadoop.IO.Compress.CompressionCodecFactory factory = new Org.Apache.Hadoop.IO.Compress.CompressionCodecFactory
				(conf);
			bool encode = false;
			for (int i = 0; i < args.Length; ++i)
			{
				if ("-in".Equals(args[i]))
				{
					encode = true;
				}
				else
				{
					if ("-out".Equals(args[i]))
					{
						encode = false;
					}
					else
					{
						CompressionCodec codec = factory.GetCodec(new Path(args[i]));
						if (codec == null)
						{
							System.Console.Out.WriteLine("Codec for " + args[i] + " not found.");
						}
						else
						{
							if (encode)
							{
								CompressionOutputStream @out = null;
								InputStream @in = null;
								try
								{
									@out = codec.CreateOutputStream(new FileOutputStream(args[i]));
									byte[] buffer = new byte[100];
									string inFilename = RemoveSuffix(args[i], codec.GetDefaultExtension());
									@in = new FileInputStream(inFilename);
									int len = @in.Read(buffer);
									while (len > 0)
									{
										@out.Write(buffer, 0, len);
										len = @in.Read(buffer);
									}
								}
								finally
								{
									if (@out != null)
									{
										@out.Close();
									}
									if (@in != null)
									{
										@in.Close();
									}
								}
							}
							else
							{
								CompressionInputStream @in = null;
								try
								{
									@in = codec.CreateInputStream(new FileInputStream(args[i]));
									byte[] buffer = new byte[100];
									int len = @in.Read(buffer);
									while (len > 0)
									{
										System.Console.Out.Write(buffer, 0, len);
										len = @in.Read(buffer);
									}
								}
								finally
								{
									if (@in != null)
									{
										@in.Close();
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
