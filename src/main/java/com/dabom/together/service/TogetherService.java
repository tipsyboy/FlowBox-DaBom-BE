package com.dabom.together.service;

import com.dabom.member.exception.MemberException;
import com.dabom.member.model.entity.Member;
import com.dabom.member.repository.MemberRepository;
import com.dabom.member.security.dto.MemberDetailsDto;
import com.dabom.s3.S3UrlBuilder;
import com.dabom.together.exception.TogetherException;
import com.dabom.together.model.dto.request.*;
import com.dabom.together.model.dto.response.TogetherInfoResponseDto;
import com.dabom.together.model.dto.response.TogetherListResponseDto;
import com.dabom.together.model.dto.response.TogetherMasterResponseDto;
import com.dabom.together.model.dto.response.TogetherMemberListResponseDto;
import com.dabom.together.model.entity.Together;
import com.dabom.together.model.entity.TogetherJoinMember;
import com.dabom.together.repository.TogetherJoinMemberRepository;
import com.dabom.together.repository.TogetherRepository;
import com.dabom.video.exception.VideoException;
import com.dabom.video.exception.VideoExceptionType;
import com.dabom.video.model.Video;
import com.dabom.video.repository.VideoRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Pageable;
import org.springframework.data.domain.Slice;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;
import java.util.Optional;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static com.dabom.member.exception.MemberExceptionType.MEMBER_NOT_FOUND;
import static com.dabom.together.exception.TogetherExceptionType.NOT_ACCEPT_MEMBER;
import static com.dabom.together.exception.TogetherExceptionType.NOT_MASTER_MEMBER;

@Slf4j
@Service
@Transactional(readOnly = true)
@RequiredArgsConstructor
public class TogetherService {

    private final TogetherRepository togetherRepository;
    private final TogetherJoinMemberRepository togetherJoinMemberRepository;
    private final MemberRepository memberRepository;
    private final VideoRepository videoRepository;
    private final S3UrlBuilder s3UrlBuilder;

    @Transactional
    public TogetherInfoResponseDto createTogether(TogetherCreateRequestDto dto, MemberDetailsDto memberDetailsDto) {
        Member member = memberRepository.findById(memberDetailsDto.getIdx()).orElseThrow();
        Together together = togetherRepository.save(dto.toEntity(member));
        Optional<TogetherJoinMember> optional = togetherJoinMemberRepository.findByMemberAndTogether(member, together);

        Integer videoIdx = extractVideoIdx(together.getVideoUrl());
        Video targetVideo = videoRepository.findById(videoIdx)
                .orElseThrow(() -> new VideoException(VideoExceptionType.VIDEO_NOT_FOUND));
        String videoUrl = s3UrlBuilder.buildPublicUrl(targetVideo.getSavedPath());

        log.info("videoUrl={}", videoUrl);

        if(optional.isPresent()) {
            TogetherJoinMember joinMember = optional.get();
            if(joinMember.getIsDelete()) {
                joinMember.comeBackTogether();
                togetherJoinMemberRepository.save(joinMember);

                return TogetherInfoResponseDto.toCreateDto(together, videoUrl);
            }
            return TogetherInfoResponseDto.toCreateDto(together, videoUrl);
        }

        TogetherJoinMember togetherJoinMember = TogetherJoinMember.builder()
                .isJoin(true)
                .isDelete(false)
                .member(member)
                .together(together)
                .build();
        togetherJoinMemberRepository.save(togetherJoinMember);
        return TogetherInfoResponseDto.toCreateDto(together, videoUrl);
    }

    public TogetherListResponseDto getTogetherListTest(Integer page, Integer size) {
        Pageable pageable = PageRequest.of(page, size);
        Slice<Together> openTrue = togetherRepository.findAllByIsOpenTrue(pageable);
        List<TogetherInfoResponseDto> togethers = openTrue.stream().map(TogetherInfoResponseDto::toDto).toList();

        return TogetherListResponseDto.of(togethers);
    }

    public TogetherListResponseDto searchTogetherList(String search, Integer page, Integer size) {
        Pageable pageable = PageRequest.of(page, size);
        Slice<Together> searchTogether = togetherRepository.searchAllByIsOpenTrue(search, pageable);
        List<TogetherInfoResponseDto> togethers = searchTogether.stream().map(TogetherInfoResponseDto::toDto).toList();
//        List<Together> togethers = togetherRepository.findAll();
        return TogetherListResponseDto.of(togethers);
    }

    @Transactional
    public TogetherInfoResponseDto changeTogetherTitle(Integer togetherIdx,
                                                       TogetherChangeTitleRequestDto dto,
                                                       MemberDetailsDto memberDetailsDto) {
        Together together = validMasterMember(togetherIdx, memberDetailsDto);
        together.changeTitle(dto.getTitle());
        Together save = togetherRepository.save(together);
        return TogetherInfoResponseDto.toDto(save);
    }

    @Transactional
    public TogetherInfoResponseDto changeMaxMember(Integer togetherIdx, TogetherChangeMaxMemberRequestDto dto,
                                MemberDetailsDto memberDetailsDto) {
        Together together = validMasterMember(togetherIdx, memberDetailsDto);

        together.changeMaxMemberNumber(dto.getMaxMember());
        Together save = togetherRepository.save(together);

        return TogetherInfoResponseDto.toDto(save);
    }

    @Transactional
    public TogetherInfoResponseDto changeIsOpen(Integer togetherIdx, TogetherChangeIsOpenRequestDto dto,
                                                   MemberDetailsDto memberDetailsDto) {
        Together together = validMasterMember(togetherIdx, memberDetailsDto);

        together.changeTogetherIsOpen(dto.getIsOpen());
        Together save = togetherRepository.save(together);

        return TogetherInfoResponseDto.toDto(save);
    }

    public TogetherMemberListResponseDto getTogetherMembersFromMaster(Integer togetherIdx, MemberDetailsDto memberDetailsDto) {
        Together together = validMasterMember(togetherIdx, memberDetailsDto);
        List<TogetherJoinMember> joinMembers = togetherJoinMemberRepository.findByTogether(together);
        Member master = memberRepository.findById(memberDetailsDto.getIdx())
                .orElseThrow(() -> new MemberException(MEMBER_NOT_FOUND));
        return TogetherMemberListResponseDto.toDto(joinMembers, master);
    }

    public TogetherMasterResponseDto isMaster(Integer togetherIdx, MemberDetailsDto memberDetailsDto) {
        Together together = validMasterMember(togetherIdx, memberDetailsDto);

        return TogetherMasterResponseDto.isMasterMember();
    }

    @Transactional
    public TogetherInfoResponseDto changeVideo(Integer togetherIdx, TogetherChangeVideoRequestDto dto,
                                                MemberDetailsDto memberDetailsDto) {
        Together together = validMasterMember(togetherIdx, memberDetailsDto);

        together.changeVideo(dto.getVideoUrl());
        Together save = togetherRepository.save(together);

        return TogetherInfoResponseDto.toDto(save);
    }

    @Transactional
    public TogetherInfoResponseDto kickTogetherMember(Integer togetherIdx, Integer memberIdx,
                                   MemberDetailsDto memberDetailsDto) {
        Together together = validMasterMember(togetherIdx, memberDetailsDto);

        Member member = memberRepository.findById(memberIdx)
                .orElseThrow(() -> new MemberException(MEMBER_NOT_FOUND));
        TogetherJoinMember kickMember = togetherJoinMemberRepository.findByMemberAndTogether(member, together)
                .orElseThrow(() -> new TogetherException(NOT_ACCEPT_MEMBER));
        if(kickMember.getIsDelete()) {
            return TogetherInfoResponseDto.toDto(together);
        }

        kickMember.expel();
        togetherJoinMemberRepository.save(kickMember);
        together.leaveMember();
        Together save = togetherRepository.save(together);
        return TogetherInfoResponseDto.toDto(save);
    }

    @Transactional
    public void deleteTogether(Integer togetherIdx, MemberDetailsDto memberDetailsDto) {
        Together together = validMasterMember(togetherIdx, memberDetailsDto);

        together.deleteTogether();
        List<TogetherJoinMember> joinMembers = togetherJoinMemberRepository.findByTogether(together);
        joinMembers.forEach(TogetherJoinMember::leaveTogether);

        togetherJoinMemberRepository.saveAll(joinMembers);
        togetherRepository.save(together);
    }

    private Together validMasterMember(Integer togetherIdx, MemberDetailsDto memberDetailsDto) {
        Together together = togetherRepository.findById(togetherIdx).orElseThrow();
        Member member = memberRepository.findById(memberDetailsDto.getIdx())
                .orElseThrow(() -> new MemberException(MEMBER_NOT_FOUND));
        if(!together.getMaster().equals(member)) {
            throw new TogetherException(NOT_MASTER_MEMBER);
        }
        return together;
    }

    private Integer extractVideoIdx(String url) {
        if (url == null) return null;

        Pattern pattern = Pattern.compile("/video-player/(\\d+)");
        Matcher matcher = pattern.matcher(url);

        if (matcher.find()) {
            return Integer.parseInt(matcher.group(1));
        }

        return null;
    }
}
