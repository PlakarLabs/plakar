import styled from "styled-components";

const Icon = styled.img`
  position: relative;
  width: 16px;
  height: 16px;
  overflow: hidden;
  flex-shrink: 0;
  display: none;
`;
const Text1 = styled.div`
  position: relative;
  line-height: 16px;
  font-weight: 600;
`;


const BadgeslistRoot = styled.ul`
  margin: 0;
  width: 272px;
  overflow: hidden;
  display: flex;
  flex-direction: row;
  flex-wrap: wrap;
  align-items: flex-start;
  justify-content: flex-start;
  gap: 8px;
  text-align: center;
  font-size: 12px;
  color: #294b7a;
  font-family: Inter;
`;

const TagList = () => {
  return (
    <BadgeslistRoot>
      
      <Badge>
        <BadgeBase>
          <Icon alt="" src="/icon.svg" />
          <Text1>Text</Text1>
          <Icon alt="" src="/icon1.svg" />
        </BadgeBase>
      </Badge>
      <Badge>
        <BadgeBase>
          <Icon alt="" src="/icon.svg" />
          <Text1>Text</Text1>
          <Icon alt="" src="/icon1.svg" />
        </BadgeBase>
      </Badge>
      <Badge>
        <BadgeBase>
          <Icon alt="" src="/icon.svg" />
          <Text1>Text</Text1>
          <Icon alt="" src="/icon1.svg" />
        </BadgeBase>
      </Badge>
      <Badge>
        <BadgeBase>
          <Icon alt="" src="/icon.svg" />
          <Text1>Text</Text1>
          <Icon alt="" src="/icon1.svg" />
        </BadgeBase>
      </Badge>
      <Badge>
        <BadgeBase>
          <Icon alt="" src="/icon.svg" />
          <Text1>Text</Text1>
          <Icon alt="" src="/icon1.svg" />
        </BadgeBase>
      </Badge>
      <Badge>
        <BadgeBase>
          <Icon alt="" src="/icon.svg" />
          <Text1>Text</Text1>
          <Icon alt="" src="/icon1.svg" />
        </BadgeBase>
      </Badge>
      <Badge>
        <BadgeBase>
          <Icon alt="" src="/icon.svg" />
          <Text1>Text</Text1>
          <Icon alt="" src="/icon1.svg" />
        </BadgeBase>
      </Badge>
      <Badge>
        <BadgeBase>
          <Icon alt="" src="/icon.svg" />
          <Text1>Text</Text1>
          <Icon alt="" src="/icon1.svg" />
        </BadgeBase>
      </Badge>
      <Badge>
        <BadgeBase>
          <Icon alt="" src="/icon.svg" />
          <Text1>Text</Text1>
          <Icon alt="" src="/icon1.svg" />
        </BadgeBase>
      </Badge>
    </BadgeslistRoot>
  );
};

export default TagList;
