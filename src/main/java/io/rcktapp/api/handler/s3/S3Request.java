package io.rcktapp.api.handler.s3;

/**
 * @author kfrankic
 *
 */
public class S3Request
{
   private String  bucket  = null;
   private String  key    = null;
   private Integer size   = null;
   
   private String  marker   = null;
   private boolean download = false;
   private boolean meta = false;
   private String etag = null;

   public S3Request(String bucket, String key, Integer size, boolean download, boolean meta, String marker, String etag)
   {
      this.bucket = bucket;
      this.key = key;
      this.size = size;
      this.download = download;
      this.meta = meta;
      this.marker = marker;
      this.etag = etag;
   }

   public String getBucket()
   {
      return bucket;
   }

   public String getKey()
   {
      return key;
   }

   public Integer getSize()
   {
      return size;
   }

   public boolean isDownload()
   {
      return download;
   }

   public boolean isMeta()
   {
      return meta;
   }

  public String getMarker()
  {
    return marker;
  }

   public String getEtag()
   {
      return etag;
   }

}
