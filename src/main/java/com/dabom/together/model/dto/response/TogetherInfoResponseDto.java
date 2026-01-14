package com.dabom.together.model.dto.response;

import com.dabom.member.model.dto.response.MemberInfoResponseDto;
import com.dabom.member.model.entity.Member;
import com.dabom.together.model.entity.Together;

public record TogetherInfoResponseDto(Integer togetherIdx, String title, Integer maxMemberNum,
                                      Integer joinMemberNumber, MemberInfoResponseDto master,
                                      Boolean isOpen, Integer userIdx, String code, String videoUrl) {

    public static TogetherInfoResponseDto toDtoInJoin(Together together, Member joinMember, String videoUrl) {
        Member member = together.getMaster();

        return new TogetherInfoResponseDto(together.getIdx(), together.getTitle(), together.getMaxMemberNum(),
                together.getJoinMemberNum(), MemberInfoResponseDto.toDto(member), together.getIsOpen(),
                joinMember.getIdx(), together.getCode().toString(), videoUrl);
    }

    public static TogetherInfoResponseDto toCreateDto(Together together, String videoUrl) {
        Member member = together.getMaster();
        return new TogetherInfoResponseDto(together.getIdx(), together.getTitle(), together.getMaxMemberNum(),
                together.getJoinMemberNum(), MemberInfoResponseDto.toDto(member), together.getIsOpen(),
                null, null,  videoUrl);
    }

    public static TogetherInfoResponseDto toDto(Together together, String videoUrl) {
        Member member = together.getMaster();
        return new TogetherInfoResponseDto(together.getIdx(), together.getTitle(), together.getMaxMemberNum(),
                together.getJoinMemberNum(), MemberInfoResponseDto.toDto(member), together.getIsOpen(),
                null, together.getCode().toString(), videoUrl);
    }

    public static TogetherInfoResponseDto toDto(Together together) {
        Member member = together.getMaster();
        return new TogetherInfoResponseDto(together.getIdx(), together.getTitle(), together.getMaxMemberNum(),
                together.getJoinMemberNum(), MemberInfoResponseDto.toDto(member), together.getIsOpen(),
                null, together.getCode().toString(), null);
    }
}
