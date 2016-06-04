using System;
using System.Collections.Generic;
using Com.Google.Common.Cache;
using Hadoop.Common.Core.Conf;
using Hadoop.Common.Core.Util;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.Util;
using Sharpen;

namespace Org.Apache.Hadoop.IO.Compress
{
	/// <summary>
	/// A global compressor/decompressor pool used to save and reuse
	/// (possibly native) compression/decompression codecs.
	/// </summary>
	public class CodecPool
	{
		private static readonly Log Log = LogFactory.GetLog(typeof(CodecPool));

		/// <summary>
		/// A global compressor pool used to save the expensive
		/// construction/destruction of (possibly native) decompression codecs.
		/// </summary>
		private static readonly IDictionary<Type, ICollection<Compressor>> compressorPool
			 = new Dictionary<Type, ICollection<Compressor>>();

		/// <summary>
		/// A global decompressor pool used to save the expensive
		/// construction/destruction of (possibly native) decompression codecs.
		/// </summary>
		private static readonly IDictionary<Type, ICollection<Decompressor>> decompressorPool
			 = new Dictionary<Type, ICollection<Decompressor>>();

		private static LoadingCache<Type, AtomicInteger> CreateCache<T>()
		{
			System.Type klass = typeof(T);
			return CacheBuilder.NewBuilder().Build(new _CacheLoader_63());
		}

		private sealed class _CacheLoader_63 : CacheLoader<Type, AtomicInteger>
		{
			public _CacheLoader_63()
			{
			}

			/// <exception cref="System.Exception"/>
			public override AtomicInteger Load(Type key)
			{
				return new AtomicInteger();
			}
		}

		/// <summary>Map to track the number of leased compressors</summary>
		private static readonly LoadingCache<Type, AtomicInteger> compressorCounts = CreateCache
			<Compressor>();

		/// <summary>Map to tracks the number of leased decompressors</summary>
		private static readonly LoadingCache<Type, AtomicInteger> decompressorCounts = CreateCache
			<Decompressor>();

		private static T Borrow<T>(IDictionary<Type, ICollection<T>> pool, Type codecClass
			)
		{
			T codec = null;
			// Check if an appropriate codec is available
			ICollection<T> codecSet;
			lock (pool)
			{
				codecSet = pool[codecClass];
			}
			if (codecSet != null)
			{
				lock (codecSet)
				{
					if (!codecSet.IsEmpty())
					{
						codec = codecSet.GetEnumerator().Next();
						codecSet.Remove(codec);
					}
				}
			}
			return codec;
		}

		private static bool Payback<T>(IDictionary<Type, ICollection<T>> pool, T codec)
		{
			if (codec != null)
			{
				Type codecClass = ReflectionUtils.GetClass(codec);
				ICollection<T> codecSet;
				lock (pool)
				{
					codecSet = pool[codecClass];
					if (codecSet == null)
					{
						codecSet = new HashSet<T>();
						pool[codecClass] = codecSet;
					}
				}
				lock (codecSet)
				{
					return codecSet.AddItem(codec);
				}
			}
			return false;
		}

		private static int GetLeaseCount<T>(LoadingCache<Type, AtomicInteger> usageCounts
			, Type codecClass)
		{
			return usageCounts.GetUnchecked((Type)codecClass).Get();
		}

		private static void UpdateLeaseCount<T>(LoadingCache<Type, AtomicInteger> usageCounts
			, T codec, int delta)
		{
			if (codec != null)
			{
				Type codecClass = ReflectionUtils.GetClass(codec);
				usageCounts.GetUnchecked(codecClass).AddAndGet(delta);
			}
		}

		/// <summary>
		/// Get a
		/// <see cref="Compressor"/>
		/// for the given
		/// <see cref="CompressionCodec"/>
		/// from the
		/// pool or a new one.
		/// </summary>
		/// <param name="codec">
		/// the <code>CompressionCodec</code> for which to get the
		/// <code>Compressor</code>
		/// </param>
		/// <param name="conf">the <code>Configuration</code> object which contains confs for creating or reinit the compressor
		/// 	</param>
		/// <returns>
		/// <code>Compressor</code> for the given
		/// <code>CompressionCodec</code> from the pool or a new one
		/// </returns>
		public static Compressor GetCompressor(CompressionCodec codec, Configuration conf
			)
		{
			Compressor compressor = Borrow(compressorPool, codec.GetCompressorType());
			if (compressor == null)
			{
				compressor = codec.CreateCompressor();
				Log.Info("Got brand-new compressor [" + codec.GetDefaultExtension() + "]");
			}
			else
			{
				compressor.Reinit(conf);
				if (Log.IsDebugEnabled())
				{
					Log.Debug("Got recycled compressor");
				}
			}
			UpdateLeaseCount(compressorCounts, compressor, 1);
			return compressor;
		}

		public static Compressor GetCompressor(CompressionCodec codec)
		{
			return GetCompressor(codec, null);
		}

		/// <summary>
		/// Get a
		/// <see cref="Decompressor"/>
		/// for the given
		/// <see cref="CompressionCodec"/>
		/// from the
		/// pool or a new one.
		/// </summary>
		/// <param name="codec">
		/// the <code>CompressionCodec</code> for which to get the
		/// <code>Decompressor</code>
		/// </param>
		/// <returns>
		/// <code>Decompressor</code> for the given
		/// <code>CompressionCodec</code> the pool or a new one
		/// </returns>
		public static Decompressor GetDecompressor(CompressionCodec codec)
		{
			Decompressor decompressor = Borrow(decompressorPool, codec.GetDecompressorType());
			if (decompressor == null)
			{
				decompressor = codec.CreateDecompressor();
				Log.Info("Got brand-new decompressor [" + codec.GetDefaultExtension() + "]");
			}
			else
			{
				if (Log.IsDebugEnabled())
				{
					Log.Debug("Got recycled decompressor");
				}
			}
			UpdateLeaseCount(decompressorCounts, decompressor, 1);
			return decompressor;
		}

		/// <summary>
		/// Return the
		/// <see cref="Compressor"/>
		/// to the pool.
		/// </summary>
		/// <param name="compressor">the <code>Compressor</code> to be returned to the pool</param>
		public static void ReturnCompressor(Compressor compressor)
		{
			if (compressor == null)
			{
				return;
			}
			// if the compressor can't be reused, don't pool it.
			if (compressor.GetType().IsAnnotationPresent(typeof(DoNotPool)))
			{
				return;
			}
			compressor.Reset();
			if (Payback(compressorPool, compressor))
			{
				UpdateLeaseCount(compressorCounts, compressor, -1);
			}
		}

		/// <summary>
		/// Return the
		/// <see cref="Decompressor"/>
		/// to the pool.
		/// </summary>
		/// <param name="decompressor">
		/// the <code>Decompressor</code> to be returned to the
		/// pool
		/// </param>
		public static void ReturnDecompressor(Decompressor decompressor)
		{
			if (decompressor == null)
			{
				return;
			}
			// if the decompressor can't be reused, don't pool it.
			if (decompressor.GetType().IsAnnotationPresent(typeof(DoNotPool)))
			{
				return;
			}
			decompressor.Reset();
			if (Payback(decompressorPool, decompressor))
			{
				UpdateLeaseCount(decompressorCounts, decompressor, -1);
			}
		}

		/// <summary>
		/// Return the number of leased
		/// <see cref="Compressor"/>
		/// s for this
		/// <see cref="CompressionCodec"/>
		/// </summary>
		public static int GetLeasedCompressorsCount(CompressionCodec codec)
		{
			return (codec == null) ? 0 : GetLeaseCount(compressorCounts, codec.GetCompressorType
				());
		}

		/// <summary>
		/// Return the number of leased
		/// <see cref="Decompressor"/>
		/// s for this
		/// <see cref="CompressionCodec"/>
		/// </summary>
		public static int GetLeasedDecompressorsCount(CompressionCodec codec)
		{
			return (codec == null) ? 0 : GetLeaseCount(decompressorCounts, codec.GetDecompressorType
				());
		}
	}
}
