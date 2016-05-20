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
using Sharpen;

namespace org.apache.hadoop.io.compress
{
	/// <summary>A factory that will find the correct codec for a given filename.</summary>
	public class CompressionCodecFactory
	{
		public static readonly org.apache.commons.logging.Log LOG = org.apache.commons.logging.LogFactory
			.getLog(Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.io.compress.CompressionCodecFactory
			)).getName());

		private static readonly java.util.ServiceLoader<org.apache.hadoop.io.compress.CompressionCodec
			> CODEC_PROVIDERS = java.util.ServiceLoader.load<org.apache.hadoop.io.compress.CompressionCodec
			>();

		/// <summary>A map from the reversed filename suffixes to the codecs.</summary>
		/// <remarks>
		/// A map from the reversed filename suffixes to the codecs.
		/// This is probably overkill, because the maps should be small, but it
		/// automatically supports finding the longest matching suffix.
		/// </remarks>
		private System.Collections.Generic.SortedDictionary<string, org.apache.hadoop.io.compress.CompressionCodec
			> codecs = null;

		/// <summary>A map from the reversed filename suffixes to the codecs.</summary>
		/// <remarks>
		/// A map from the reversed filename suffixes to the codecs.
		/// This is probably overkill, because the maps should be small, but it
		/// automatically supports finding the longest matching suffix.
		/// </remarks>
		private System.Collections.Generic.IDictionary<string, org.apache.hadoop.io.compress.CompressionCodec
			> codecsByName = null;

		/// <summary>A map from class names to the codecs</summary>
		private System.Collections.Generic.Dictionary<string, org.apache.hadoop.io.compress.CompressionCodec
			> codecsByClassName = null;

		private void addCodec(org.apache.hadoop.io.compress.CompressionCodec codec)
		{
			string suffix = codec.getDefaultExtension();
			codecs[((java.lang.StringBuilder)new java.lang.StringBuilder(suffix).reverse()).ToString
				()] = codec;
			codecsByClassName[Sharpen.Runtime.getClassForObject(codec).getCanonicalName()] = 
				codec;
			string codecName = Sharpen.Runtime.getClassForObject(codec).getSimpleName();
			codecsByName[org.apache.hadoop.util.StringUtils.toLowerCase(codecName)] = codec;
			if (codecName.EndsWith("Codec"))
			{
				codecName = Sharpen.Runtime.substring(codecName, 0, codecName.Length - "Codec".Length
					);
				codecsByName[org.apache.hadoop.util.StringUtils.toLowerCase(codecName)] = codec;
			}
		}

		/// <summary>Print the extension map out as a string.</summary>
		public override string ToString()
		{
			java.lang.StringBuilder buf = new java.lang.StringBuilder();
			System.Collections.Generic.IEnumerator<System.Collections.Generic.KeyValuePair<string
				, org.apache.hadoop.io.compress.CompressionCodec>> itr = codecs.GetEnumerator();
			buf.Append("{ ");
			if (itr.MoveNext())
			{
				System.Collections.Generic.KeyValuePair<string, org.apache.hadoop.io.compress.CompressionCodec
					> entry = itr.Current;
				buf.Append(entry.Key);
				buf.Append(": ");
				buf.Append(Sharpen.Runtime.getClassForObject(entry.Value).getName());
				while (itr.MoveNext())
				{
					entry = itr.Current;
					buf.Append(", ");
					buf.Append(entry.Key);
					buf.Append(": ");
					buf.Append(Sharpen.Runtime.getClassForObject(entry.Value).getName());
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
		public static System.Collections.Generic.IList<java.lang.Class> getCodecClasses(org.apache.hadoop.conf.Configuration
			 conf)
		{
			System.Collections.Generic.IList<java.lang.Class> result = new System.Collections.Generic.List
				<java.lang.Class>();
			// Add codec classes discovered via service loading
			lock (CODEC_PROVIDERS)
			{
				// CODEC_PROVIDERS is a lazy collection. Synchronize so it is
				// thread-safe. See HADOOP-8406.
				foreach (org.apache.hadoop.io.compress.CompressionCodec codec in CODEC_PROVIDERS)
				{
					result.add(Sharpen.Runtime.getClassForObject(codec));
				}
			}
			// Add codec classes from configuration
			string codecsString = conf.get(org.apache.hadoop.fs.CommonConfigurationKeys.IO_COMPRESSION_CODECS_KEY
				);
			if (codecsString != null)
			{
				java.util.StringTokenizer codecSplit = new java.util.StringTokenizer(codecsString
					, ",");
				while (codecSplit.MoveNext())
				{
					string codecSubstring = codecSplit.nextToken().Trim();
					if (codecSubstring.Length != 0)
					{
						try
						{
							java.lang.Class cls = conf.getClassByName(codecSubstring);
							if (!Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.io.compress.CompressionCodec
								)).isAssignableFrom(cls))
							{
								throw new System.ArgumentException("Class " + codecSubstring + " is not a CompressionCodec"
									);
							}
							result.add(cls.asSubclass<org.apache.hadoop.io.compress.CompressionCodec>());
						}
						catch (java.lang.ClassNotFoundException ex)
						{
							throw new System.ArgumentException("Compression codec " + codecSubstring + " not found."
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
		public static void setCodecClasses(org.apache.hadoop.conf.Configuration conf, System.Collections.Generic.IList
			<java.lang.Class> classes)
		{
			java.lang.StringBuilder buf = new java.lang.StringBuilder();
			System.Collections.Generic.IEnumerator<java.lang.Class> itr = classes.GetEnumerator
				();
			if (itr.MoveNext())
			{
				java.lang.Class cls = itr.Current;
				buf.Append(cls.getName());
				while (itr.MoveNext())
				{
					buf.Append(',');
					buf.Append(itr.Current.getName());
				}
			}
			conf.set(org.apache.hadoop.fs.CommonConfigurationKeys.IO_COMPRESSION_CODECS_KEY, 
				buf.ToString());
		}

		/// <summary>
		/// Find the codecs specified in the config value io.compression.codecs
		/// and register them.
		/// </summary>
		/// <remarks>
		/// Find the codecs specified in the config value io.compression.codecs
		/// and register them. Defaults to gzip and deflate.
		/// </remarks>
		public CompressionCodecFactory(org.apache.hadoop.conf.Configuration conf)
		{
			codecs = new System.Collections.Generic.SortedDictionary<string, org.apache.hadoop.io.compress.CompressionCodec
				>();
			codecsByClassName = new System.Collections.Generic.Dictionary<string, org.apache.hadoop.io.compress.CompressionCodec
				>();
			codecsByName = new System.Collections.Generic.Dictionary<string, org.apache.hadoop.io.compress.CompressionCodec
				>();
			System.Collections.Generic.IList<java.lang.Class> codecClasses = getCodecClasses(
				conf);
			if (codecClasses == null || codecClasses.isEmpty())
			{
				addCodec(new org.apache.hadoop.io.compress.GzipCodec());
				addCodec(new org.apache.hadoop.io.compress.DefaultCodec());
			}
			else
			{
				foreach (java.lang.Class codecClass in codecClasses)
				{
					addCodec(org.apache.hadoop.util.ReflectionUtils.newInstance(codecClass, conf));
				}
			}
		}

		/// <summary>
		/// Find the relevant compression codec for the given file based on its
		/// filename suffix.
		/// </summary>
		/// <param name="file">the filename to check</param>
		/// <returns>the codec object</returns>
		public virtual org.apache.hadoop.io.compress.CompressionCodec getCodec(org.apache.hadoop.fs.Path
			 file)
		{
			org.apache.hadoop.io.compress.CompressionCodec result = null;
			if (codecs != null)
			{
				string filename = file.getName();
				string reversedFilename = ((java.lang.StringBuilder)new java.lang.StringBuilder(filename
					).reverse()).ToString();
				System.Collections.Generic.SortedDictionary<string, org.apache.hadoop.io.compress.CompressionCodec
					> subMap = codecs.headMap(reversedFilename);
				if (!subMap.isEmpty())
				{
					string potentialSuffix = subMap.lastKey();
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
		public virtual org.apache.hadoop.io.compress.CompressionCodec getCodecByClassName
			(string classname)
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
		public virtual org.apache.hadoop.io.compress.CompressionCodec getCodecByName(string
			 codecName)
		{
			if (codecsByClassName == null)
			{
				return null;
			}
			org.apache.hadoop.io.compress.CompressionCodec codec = getCodecByClassName(codecName
				);
			if (codec == null)
			{
				// trying to get the codec by name in case the name was specified
				// instead a class
				codec = codecsByName[org.apache.hadoop.util.StringUtils.toLowerCase(codecName)];
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
		public virtual java.lang.Class getCodecClassByName(string codecName)
		{
			org.apache.hadoop.io.compress.CompressionCodec codec = getCodecByName(codecName);
			if (codec == null)
			{
				return null;
			}
			return Sharpen.Runtime.getClassForObject(codec);
		}

		/// <summary>Removes a suffix from a filename, if it has it.</summary>
		/// <param name="filename">the filename to strip</param>
		/// <param name="suffix">the suffix to remove</param>
		/// <returns>the shortened filename</returns>
		public static string removeSuffix(string filename, string suffix)
		{
			if (filename.EndsWith(suffix))
			{
				return Sharpen.Runtime.substring(filename, 0, filename.Length - suffix.Length);
			}
			return filename;
		}

		/// <summary>A little test program.</summary>
		/// <param name="args"/>
		/// <exception cref="System.Exception"/>
		public static void Main(string[] args)
		{
			org.apache.hadoop.conf.Configuration conf = new org.apache.hadoop.conf.Configuration
				();
			org.apache.hadoop.io.compress.CompressionCodecFactory factory = new org.apache.hadoop.io.compress.CompressionCodecFactory
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
						org.apache.hadoop.io.compress.CompressionCodec codec = factory.getCodec(new org.apache.hadoop.fs.Path
							(args[i]));
						if (codec == null)
						{
							System.Console.Out.WriteLine("Codec for " + args[i] + " not found.");
						}
						else
						{
							if (encode)
							{
								org.apache.hadoop.io.compress.CompressionOutputStream @out = null;
								java.io.InputStream @in = null;
								try
								{
									@out = codec.createOutputStream(new java.io.FileOutputStream(args[i]));
									byte[] buffer = new byte[100];
									string inFilename = removeSuffix(args[i], codec.getDefaultExtension());
									@in = new java.io.FileInputStream(inFilename);
									int len = @in.read(buffer);
									while (len > 0)
									{
										@out.write(buffer, 0, len);
										len = @in.read(buffer);
									}
								}
								finally
								{
									if (@out != null)
									{
										@out.close();
									}
									if (@in != null)
									{
										@in.close();
									}
								}
							}
							else
							{
								org.apache.hadoop.io.compress.CompressionInputStream @in = null;
								try
								{
									@in = codec.createInputStream(new java.io.FileInputStream(args[i]));
									byte[] buffer = new byte[100];
									int len = @in.read(buffer);
									while (len > 0)
									{
										System.Console.Out.write(buffer, 0, len);
										len = @in.read(buffer);
									}
								}
								finally
								{
									if (@in != null)
									{
										@in.close();
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
