package com.dabom.video.controller;


import com.dabom.common.BaseResponse;
import com.dabom.member.security.dto.MemberDetailsDto;
import com.dabom.video.constansts.SwaggerConstants;
import com.dabom.video.model.dto.VideoMetadataRequestDto;
import com.dabom.video.model.dto.score.VideoScoreRequestDto;
import com.dabom.video.service.VideoService;
import com.dabom.video.service.s3.VideoS3EncodingService;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.media.Content;
import io.swagger.v3.oas.annotations.media.ExampleObject;
import io.swagger.v3.oas.annotations.media.Schema;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import io.swagger.v3.oas.annotations.responses.ApiResponses;
import io.swagger.v3.oas.annotations.tags.Tag;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.security.core.annotation.AuthenticationPrincipal;
import org.springframework.web.bind.annotation.*;

import java.io.IOException;


@Tag(name = "비디오 메타데이터 기능", description = "비디오 정보 및 상태 관리 API")
@Slf4j
@RestController
@RequestMapping("/api/videos")
@RequiredArgsConstructor
public class VideoMetadataController {

    private final VideoService videoService;
    private final VideoS3EncodingService videoS3EncodingService;
    //    private final VideoLocalEncodingService videoLocalEncodingService;


    @Operation(summary = "비디오 메타데이터 업데이트", description = "업로드된 비디오의 제목, 설명 등 메타데이터를 업데이트합니다.")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "200", description = "메타데이터 업데이트 성공",
                    content = @Content(mediaType = "application/json",
                            schema = @Schema(implementation = BaseResponse.class),
                            examples = @ExampleObject(value = SwaggerConstants.VIDEO_METADATA_RESPONSE)))
    })
    @PatchMapping("/metadata/{videoIdx}")
    public ResponseEntity<BaseResponse<Integer>> uploadData(@PathVariable Integer videoIdx,
                                                            @io.swagger.v3.oas.annotations.parameters.RequestBody(
                                                                    description = "비디오 메타데이터 정보",
                                                                    required = true,
                                                                    content = @Content(mediaType = "application/json",
                                                                            schema = @Schema(implementation = VideoMetadataRequestDto.class),
                                                                            examples = @ExampleObject(value = SwaggerConstants.VIDEO_METADATA_REQUEST)
                                                                    )
                                                            )
                                                            @RequestBody VideoMetadataRequestDto requestDto) throws IOException, InterruptedException {

        Integer i = videoService.mappingMetadata(requestDto);

//        videoS3EncodingService.encode(videoIdx);

        return ResponseEntity.ok(BaseResponse.of(i, HttpStatus.OK));
    }

    @Operation(summary = "비디오 공개 상태 변경", description = "비디오의 공개/비공개 상태를 변경합니다.")
    @PatchMapping("/{videoIdx}/visibility")
    public ResponseEntity<BaseResponse<Integer>> toggleVideoVisibility(@PathVariable Integer videoIdx,
                                                                       @AuthenticationPrincipal MemberDetailsDto loginMember) {
        Integer idx = videoService.toggleVideoVisibility(videoIdx, loginMember.getIdx());
        return ResponseEntity.ok(BaseResponse.of(idx, HttpStatus.OK, "비디오 상태가 성공적으로 변경되었습니다."));
    }

    @Operation(summary = "비디오 점수 평가", description = "비디오에 대한 점수를 평가합니다.")
    @PostMapping("/{videoIdx}/score")
    public ResponseEntity<BaseResponse<Void>> score(@PathVariable Integer videoIdx,
                                                    @AuthenticationPrincipal MemberDetailsDto loginMember,
                                                    @RequestBody VideoScoreRequestDto requestDto) {

        videoService.scoreVideo(requestDto, loginMember.getIdx(), videoIdx);
        return ResponseEntity.ok(BaseResponse.of(null, HttpStatus.OK, "영상 평가가 완료되었습니다."));
    }
}