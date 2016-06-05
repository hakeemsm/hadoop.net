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
using System.IO;
using Com.Google.Common.Base;
using Org.Apache.Commons.Lang;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.IO;
using Org.Apache.Hadoop.Registry.Client.Exceptions;
using Org.Codehaus.Jackson;
using Org.Codehaus.Jackson.Map;
using Org.Slf4j;
using Sharpen;

namespace Org.Apache.Hadoop.Registry.Client.Binding
{
	/// <summary>Support for marshalling objects to and from JSON.</summary>
	/// <remarks>
	/// Support for marshalling objects to and from JSON.
	/// <p>
	/// It constructs an object mapper as an instance field.
	/// and synchronizes access to those methods
	/// which use the mapper
	/// </remarks>
	/// <?/>
	public class JsonSerDeser<T>
	{
		private static readonly Logger Log = LoggerFactory.GetLogger(typeof(Org.Apache.Hadoop.Registry.Client.Binding.JsonSerDeser
			));

		private const string Utf8 = "UTF-8";

		public const string ENoData = "No data at path";

		public const string EDataTooShort = "Data at path too short";

		public const string EMissingMarkerString = "Missing marker string: ";

		private readonly Type classType;

		private readonly ObjectMapper mapper;

		/// <summary>Create an instance bound to a specific type</summary>
		/// <param name="classType">class to marshall</param>
		public JsonSerDeser(Type classType)
		{
			Preconditions.CheckArgument(classType != null, "null classType");
			this.classType = classType;
			this.mapper = new ObjectMapper();
			mapper.Configure(DeserializationConfig.Feature.FailOnUnknownProperties, false);
		}

		/// <summary>Get the simple name of the class type to be marshalled</summary>
		/// <returns>the name of the class being marshalled</returns>
		public virtual string GetName()
		{
			return classType.Name;
		}

		/// <summary>Convert from JSON</summary>
		/// <param name="json">input</param>
		/// <returns>the parsed JSON</returns>
		/// <exception cref="System.IO.IOException">IO</exception>
		/// <exception cref="Org.Codehaus.Jackson.Map.JsonMappingException">failure to map from the JSON to this class
		/// 	</exception>
		/// <exception cref="Org.Codehaus.Jackson.JsonParseException"/>
		public virtual T FromJson(string json)
		{
			lock (this)
			{
				try
				{
					return mapper.ReadValue(json, classType);
				}
				catch (IOException e)
				{
					Log.Error("Exception while parsing json : " + e + "\n" + json, e);
					throw;
				}
			}
		}

		/// <summary>Convert from a JSON file</summary>
		/// <param name="jsonFile">input file</param>
		/// <returns>the parsed JSON</returns>
		/// <exception cref="System.IO.IOException">IO problems</exception>
		/// <exception cref="Org.Codehaus.Jackson.Map.JsonMappingException">failure to map from the JSON to this class
		/// 	</exception>
		/// <exception cref="Org.Codehaus.Jackson.JsonParseException"/>
		public virtual T FromFile(FilePath jsonFile)
		{
			lock (this)
			{
				try
				{
					return mapper.ReadValue(jsonFile, classType);
				}
				catch (IOException e)
				{
					Log.Error("Exception while parsing json file {}: {}", jsonFile, e);
					throw;
				}
			}
		}

		/// <summary>Convert from a JSON file</summary>
		/// <param name="resource">input file</param>
		/// <returns>the parsed JSON</returns>
		/// <exception cref="System.IO.IOException">IO problems</exception>
		/// <exception cref="Org.Codehaus.Jackson.Map.JsonMappingException">failure to map from the JSON to this class
		/// 	</exception>
		/// <exception cref="Org.Codehaus.Jackson.JsonParseException"/>
		public virtual T FromResource(string resource)
		{
			lock (this)
			{
				InputStream resStream = null;
				try
				{
					resStream = this.GetType().GetResourceAsStream(resource);
					if (resStream == null)
					{
						throw new FileNotFoundException(resource);
					}
					return mapper.ReadValue(resStream, classType);
				}
				catch (IOException e)
				{
					Log.Error("Exception while parsing json resource {}: {}", resource, e);
					throw;
				}
				finally
				{
					IOUtils.CloseStream(resStream);
				}
			}
		}

		/// <summary>clone by converting to JSON and back again.</summary>
		/// <remarks>
		/// clone by converting to JSON and back again.
		/// This is much less efficient than any Java clone process.
		/// </remarks>
		/// <param name="instance">instance to duplicate</param>
		/// <returns>a new instance</returns>
		/// <exception cref="System.IO.IOException">problems.</exception>
		public virtual T FromInstance(T instance)
		{
			return FromJson(ToJson(instance));
		}

		/// <summary>Load from a Hadoop filesystem</summary>
		/// <param name="fs">filesystem</param>
		/// <param name="path">path</param>
		/// <returns>a loaded CD</returns>
		/// <exception cref="System.IO.IOException">IO problems</exception>
		/// <exception cref="System.IO.EOFException">if not enough bytes were read in</exception>
		/// <exception cref="Org.Codehaus.Jackson.JsonParseException">parse problems</exception>
		/// <exception cref="Org.Codehaus.Jackson.Map.JsonMappingException">O/J mapping problems
		/// 	</exception>
		public virtual T Load(FileSystem fs, Path path)
		{
			FileStatus status = fs.GetFileStatus(path);
			long len = status.GetLen();
			byte[] b = new byte[(int)len];
			FSDataInputStream dataInputStream = fs.Open(path);
			int count = dataInputStream.Read(b);
			if (count != len)
			{
				throw new EOFException(path.ToString() + ": read finished prematurely");
			}
			return FromBytes(path.ToString(), b);
		}

		/// <summary>Save a cluster description to a hadoop filesystem</summary>
		/// <param name="fs">filesystem</param>
		/// <param name="path">path</param>
		/// <param name="overwrite">should any existing file be overwritten</param>
		/// <exception cref="System.IO.IOException">IO exception</exception>
		public virtual void Save(FileSystem fs, Path path, T instance, bool overwrite)
		{
			FSDataOutputStream dataOutputStream = fs.Create(path, overwrite);
			WriteJsonAsBytes(instance, dataOutputStream);
		}

		/// <summary>Write the json as bytes -then close the file</summary>
		/// <param name="dataOutputStream">an outout stream that will always be closed</param>
		/// <exception cref="System.IO.IOException">on any failure</exception>
		private void WriteJsonAsBytes(T instance, DataOutputStream dataOutputStream)
		{
			try
			{
				byte[] b = ToBytes(instance);
				dataOutputStream.Write(b);
			}
			finally
			{
				dataOutputStream.Close();
			}
		}

		/// <summary>Convert JSON To bytes</summary>
		/// <param name="instance">instance to convert</param>
		/// <returns>a byte array</returns>
		/// <exception cref="System.IO.IOException"/>
		public virtual byte[] ToBytes(T instance)
		{
			string json = ToJson(instance);
			return Sharpen.Runtime.GetBytesForString(json, Utf8);
		}

		/// <summary>Deserialize from a byte array</summary>
		/// <param name="path">path the data came from</param>
		/// <param name="bytes">byte array</param>
		/// <exception cref="System.IO.IOException">all problems</exception>
		/// <exception cref="System.IO.EOFException">not enough data</exception>
		/// <exception cref="Org.Apache.Hadoop.Registry.Client.Exceptions.InvalidRecordException
		/// 	">if the parsing failed -the record is invalid</exception>
		public virtual T FromBytes(string path, byte[] bytes)
		{
			return FromBytes(path, bytes, string.Empty);
		}

		/// <summary>Deserialize from a byte array, optionally checking for a marker string.</summary>
		/// <remarks>
		/// Deserialize from a byte array, optionally checking for a marker string.
		/// <p>
		/// If the marker parameter is supplied (and not empty), then its presence
		/// will be verified before the JSON parsing takes place; it is a fast-fail
		/// check. If not found, an
		/// <see cref="Org.Apache.Hadoop.Registry.Client.Exceptions.InvalidRecordException"/>
		/// exception will be
		/// raised
		/// </remarks>
		/// <param name="path">path the data came from</param>
		/// <param name="bytes">byte array</param>
		/// <param name="marker">
		/// an optional string which, if set, MUST be present in the
		/// UTF-8 parsed payload.
		/// </param>
		/// <returns>The parsed record</returns>
		/// <exception cref="System.IO.IOException">all problems</exception>
		/// <exception cref="System.IO.EOFException">not enough data</exception>
		/// <exception cref="Org.Apache.Hadoop.Registry.Client.Exceptions.InvalidRecordException
		/// 	">if the JSON parsing failed.</exception>
		/// <exception cref="Org.Apache.Hadoop.Registry.Client.Exceptions.NoRecordException">
		/// if the data is not considered a record: either
		/// it is too short or it did not contain the marker string.
		/// </exception>
		public virtual T FromBytes(string path, byte[] bytes, string marker)
		{
			int len = bytes.Length;
			if (len == 0)
			{
				throw new NoRecordException(path, ENoData);
			}
			if (StringUtils.IsNotEmpty(marker) && len < marker.Length)
			{
				throw new NoRecordException(path, EDataTooShort);
			}
			string json = Sharpen.Runtime.GetStringForBytes(bytes, 0, len, Utf8);
			if (StringUtils.IsNotEmpty(marker) && !json.Contains(marker))
			{
				throw new NoRecordException(path, EMissingMarkerString + marker);
			}
			try
			{
				return FromJson(json);
			}
			catch (JsonProcessingException e)
			{
				throw new InvalidRecordException(path, e.ToString(), e);
			}
		}

		/// <summary>Convert an instance to a JSON string</summary>
		/// <param name="instance">instance to convert</param>
		/// <returns>a JSON string description</returns>
		/// <exception cref="Org.Codehaus.Jackson.JsonParseException">parse problems</exception>
		/// <exception cref="Org.Codehaus.Jackson.Map.JsonMappingException">O/J mapping problems
		/// 	</exception>
		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="Org.Codehaus.Jackson.JsonGenerationException"/>
		public virtual string ToJson(T instance)
		{
			lock (this)
			{
				mapper.Configure(SerializationConfig.Feature.IndentOutput, true);
				return mapper.WriteValueAsString(instance);
			}
		}

		/// <summary>Convert an instance to a string form for output.</summary>
		/// <remarks>
		/// Convert an instance to a string form for output. This is a robust
		/// operation which will convert any JSON-generating exceptions into
		/// error text.
		/// </remarks>
		/// <param name="instance">non-null instance</param>
		/// <returns>a JSON string</returns>
		public virtual string ToString(T instance)
		{
			Preconditions.CheckArgument(instance != null, "Null instance argument");
			try
			{
				return ToJson(instance);
			}
			catch (IOException e)
			{
				return "Failed to convert to a string: " + e;
			}
		}
	}
}
