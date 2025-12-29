package com.dabom.video.service.s3;

import com.dabom.s3.S3FileManager;
import com.dabom.s3.S3PresignedUrlInformationDto;
import com.dabom.member.exception.MemberException;
import com.dabom.member.exception.MemberExceptionType;
import com.dabom.member.model.entity.Member;
import com.dabom.member.repository.MemberRepository;
import com.dabom.video.exception.VideoException;
import com.dabom.video.exception.VideoExceptionType;
import com.dabom.video.model.Video;
import com.dabom.video.model.VideoStatus;
import com.dabom.s3.PresignedUrlRequestDto;
import com.dabom.video.model.dto.VideoPresignedUrlResponseDto;
import com.dabom.video.repository.VideoRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.util.UUID;

@Slf4j
@Service
@RequiredArgsConstructor
public class VideoS3UploadService {

    private static final Long MAX_FILE_SIZE = 1024 * 1024 * 100L;
    private static final String VIDEO_TEMP_PATH = "videos/original/";
    private static final String SAVED_TEMP_PATH = "videos/hls/";

    private final VideoRepository videoRepository;
    private final MemberRepository memberRepository;
    private final S3FileManager s3FileManager;

    public VideoPresignedUrlResponseDto generatePresignedUrl(PresignedUrlRequestDto requestDto, Integer memberIdx) {
        Member channel = memberRepository.findById(memberIdx)
                .orElseThrow(() -> new MemberException(MemberExceptionType.MEMBER_NOT_FOUND));

        // 1. 파일 검증
//        validateFile(request);

        // 2. S3 Key 생성 (UUID 기반)
        String originalFileName = requestDto.originalFilename();
        String s3OriginalFilePathPrefix = s3FileManager.generateS3PathPrefix(VIDEO_TEMP_PATH);
        String uuid = UUID.randomUUID().toString();
        String originalFileS3Key = s3OriginalFilePathPrefix + uuid + "." + extractExtension(originalFileName);

        String s3EncodedFilePathPrefix = s3FileManager.generateS3PathPrefix(SAVED_TEMP_PATH);
        String encodedFileS3Key = s3EncodedFilePathPrefix + uuid + "/index.m3u8";

        // 3. Video Entity 생성
        Integer videoIdx = createVideoEntity(requestDto, originalFileS3Key, channel, encodedFileS3Key);

        // 4. Presigned URL 생성
        S3PresignedUrlInformationDto presignedUrlInfo = s3FileManager.createPresignedUrl(originalFileS3Key, requestDto.contentType());

        return VideoPresignedUrlResponseDto.builder()
                .videoIdx(videoIdx)
                .uploadUrl(presignedUrlInfo.uploadUrl())
                .s3Key(originalFileS3Key)
                .expiresIn(presignedUrlInfo.expiresIn())
                .build();
    }

    private Integer createVideoEntity(PresignedUrlRequestDto requestDto, String s3Key, Member channel, String savedS3Key) {
        Video video = Video.builder()
                .channel(channel)
                .originalFilename(requestDto.originalFilename())
                .originalPath(s3Key)
                .originalSize(requestDto.fileSize())
                .savedPath(savedS3Key)
                .status(VideoStatus.UPLOADING)
                .contentType(requestDto.contentType())
                .build();
        return videoRepository.save(video).getIdx();
    }

    private String extractExtension(String fileName) {
        int dotIndex = fileName.lastIndexOf(".");
        if (dotIndex == -1) {
            throw new VideoException(VideoExceptionType.MISSING_FILE_EXTENSION);
        }
        return fileName.substring(dotIndex + 1).toLowerCase();
    }


    private void validateFile(PresignedUrlRequestDto requestDto) {
        // 파일 크기 검증 (100MB 제한)
        if (requestDto.fileSize() > MAX_FILE_SIZE) {
            throw new VideoException(VideoExceptionType.FILE_SIZE_EXCEEDED);
        }

        // 컨텐츠 타입 검증
        if (!requestDto.contentType().startsWith("video/")) {
            throw new VideoException(VideoExceptionType.INVALID_CONTENT_TYPE);
        }

        // 지원 확장자 검증
        String extension = extractExtension(requestDto.originalFilename());
        if (!isValidVideoExtension(extension)) {
            throw new VideoException(VideoExceptionType.UNSUPPORTED_VIDEO_FORMAT);
        }
    }

    private boolean isValidVideoExtension(String extension) {
//        mp4|avi|mkv|mov|wmv|flv|webm|m4v
        return extension.matches("mp4");
    }
}