using System.Collections.Generic;
using Com.Google.Common.Collect;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Webapp
{
	/// <summary>A class to help passing around request scoped info</summary>
	public class ResponseInfo : IEnumerable<ResponseInfo.Item>
	{
		public class Item
		{
			public readonly string key;

			public readonly string url;

			public readonly object value;

			public readonly bool isRaw;

			internal Item(string key, string url, object value, bool isRaw)
			{
				this.key = key;
				this.url = url;
				this.value = value;
				this.isRaw = isRaw;
			}

			public static ResponseInfo.Item Of(string key, object value, bool isRaw)
			{
				return new ResponseInfo.Item(key, null, value, isRaw);
			}

			public static ResponseInfo.Item Of(string key, string url, object value)
			{
				return new ResponseInfo.Item(key, url, value, false);
			}
		}

		internal readonly IList<ResponseInfo.Item> items = Lists.NewArrayList();

		internal string about = "Info";

		// Do NOT add any constructors here, unless...
		public static ResponseInfo $about(string about)
		{
			ResponseInfo info = new ResponseInfo();
			info.about = about;
			return info;
		}

		public virtual ResponseInfo About(string about)
		{
			this.about = about;
			return this;
		}

		public virtual string About()
		{
			return about;
		}

		public virtual ResponseInfo (string key, object value)
		{
			items.AddItem(ResponseInfo.Item.Of(key, value, false));
			return this;
		}

		public virtual ResponseInfo (string key, string url, object anchor)
		{
			if (url == null)
			{
				items.AddItem(ResponseInfo.Item.Of(key, anchor, false));
			}
			else
			{
				items.AddItem(ResponseInfo.Item.Of(key, url, anchor));
			}
			return this;
		}

		//Value is raw HTML and shouldn't be escaped
		public virtual ResponseInfo _r(string key, object value)
		{
			items.AddItem(ResponseInfo.Item.Of(key, value, true));
			return this;
		}

		public virtual void Clear()
		{
			items.Clear();
		}

		public override IEnumerator<ResponseInfo.Item> GetEnumerator()
		{
			return items.GetEnumerator();
		}
	}
}
