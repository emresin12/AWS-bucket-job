package awswrapper

import (
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
)

type S3ClientConfig struct {
	AccessKey  string
	SecretKey  string
	Region     string
	BucketName string
}

type S3Client struct {
	svc        *s3.S3
	bucketName string
}

func (cfg *S3ClientConfig) NewS3Client() (*S3Client, error) {

	sess, err := session.NewSession(&aws.Config{
		Region:      aws.String(cfg.Region),
		Credentials: credentials.NewStaticCredentials(cfg.AccessKey, cfg.SecretKey, ""),
	})
	if err != nil {
		return nil, err
	}

	s3Client := new(S3Client)
	s3Client.svc = s3.New(sess)
	s3Client.bucketName = cfg.BucketName

	return s3Client, nil

}

func (client *S3Client) GetObjectFromBucket(objectKey string) (*s3.GetObjectOutput, error) {
	objectOutput, err := client.svc.GetObject(&s3.GetObjectInput{
		Bucket: aws.String(client.bucketName),
		Key:    aws.String(objectKey)},
	)
	if err != nil {
		return nil, err
	}

	return objectOutput, nil
}
