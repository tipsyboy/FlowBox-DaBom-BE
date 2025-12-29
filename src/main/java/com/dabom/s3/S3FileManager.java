package com.dabom.s3;

import com.dabom.video.exception.VideoException;
import com.dabom.video.exception.VideoExceptionType;
import lombok.RequiredArgsConstructor;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.presigner.S3Presigner;
import software.amazon.awssdk.services.s3.presigner.model.PresignedPutObjectRequest;
import software.amazon.awssdk.services.s3.presigner.model.PutObjectPresignRequest;

import java.time.Duration;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.UUID;


@Component
@RequiredArgsConstructor
public class S3FileManager {

    @Value("${spring.cloud.aws.s3.bucket}")
    private String bucketName;

    @Value("${spring.cloud.aws.s3.presigned-url-duration}")
    private int presignedUrlDuration;

    private final S3Presigner s3Presigner;

    public S3PresignedUrlInformationDto createPresignedUrl(String s3Key, String contentType) {
        PutObjectRequest putObjectRequest = PutObjectRequest.builder()
                .bucket(bucketName)
                .key(s3Key)
                .contentType(contentType)
                .build();

        PutObjectPresignRequest presignRequest = PutObjectPresignRequest.builder()
                .signatureDuration(Duration.ofSeconds(presignedUrlDuration))
                .putObjectRequest(putObjectRequest)
                .build();

        PresignedPutObjectRequest presignedRequest = s3Presigner.presignPutObject(presignRequest);

        return S3PresignedUrlInformationDto.builder()
                .uploadUrl(presignedRequest.url().toString())
                .expiresIn(presignedUrlDuration)
                .build();
    }

    public String generateS3Key(String originalFileName, String uploadPath) {
        String todayPath = LocalDate.now()
                .format(DateTimeFormatter.ofPattern("yyyy/MM/dd"));
        String extension = extractExtension(originalFileName);
        String uuid = UUID.randomUUID().toString();

        return uploadPath + todayPath + "/" + uuid + "." + extension;
    }

    public String generateS3PathPrefix(String uploadPath) {
        String todayPath = LocalDate.now()
                .format(DateTimeFormatter.ofPattern("yyyy/MM/dd"));

        return uploadPath + todayPath + "/";
    }

    private String extractExtension(String fileName) {
        int dotIndex = fileName.lastIndexOf(".");
        if (dotIndex == -1) {
            throw new VideoException(VideoExceptionType.MISSING_FILE_EXTENSION);
        }
        return fileName.substring(dotIndex + 1).toLowerCase();
    }
}
